// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Resolving [`ResourceRef`] / [`ResourceSelector`] into a repository scope.
//!
//! Both types are ODF's, defined in the domain crate — a ref names exactly one
//! resource by exact name, a selector matches zero or many by SQL `LIKE`
//! pattern and labels. Several selectors in one call act as a logical OR, which
//! is what lets one call span several types and — from stage 5 onwards —
//! several accounts.
//!
//! Type resolution accepts one vocabulary throughout: canonical selectors
//! (`variablesets`), aliases (`vs`), the ODF type name (`VariableSet`), and the
//! full schema URI. That is what lets an ODF [`TypeRef`] resolve directly
//! rather than needing a facade-local twin. See
//! [`ResourceTypeDescriptor::matches_selector`].
//!
//! [`ResourceTypeDescriptor::matches_selector`]: kamu_resources::ResourceTypeDescriptor::matches_selector

use std::collections::BTreeMap;

use kamu_resources::{
    ResourceID,
    ResourceName,
    ResourceQuery,
    ResourceRef,
    ResourceScope,
    ResourceSelector,
    ResourceTypeQuery,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A field ODF defines that the facade cannot honour, rejected rather than
/// ignored so a caller who sets it learns it had no effect.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum UnsupportedSelectorFieldError {
    /// Forward-reserved in ODF for when datasets and accounts become
    /// resources; no repository can resolve by it today.
    #[error("Resource `did` is reserved for future use and cannot be resolved yet")]
    Did,

    #[error("Resource reference must specify at least one of `id` or `name`")]
    EmptyRef,
}

/// Rejects a ref the facade cannot resolve: one carrying a `did`, or one
/// naming nothing at all.
///
/// A type-less ref is *not* rejected — it resolves across every registered
/// type, the same way a type-less selector does.
pub fn validate_ref(value: &ResourceRef) -> Result<(), UnsupportedSelectorFieldError> {
    if value.did.is_some() {
        return Err(UnsupportedSelectorFieldError::Did);
    }
    if value.id.is_none() && value.name.is_none() {
        return Err(UnsupportedSelectorFieldError::EmptyRef);
    }
    Ok(())
}

/// Rejects a selector carrying a `did` the facade cannot resolve.
///
/// The selector gained `did` when ODF made the type optional. Unlike a ref, a
/// selector narrowing by nothing is meaningful — it matches every resource of
/// its type — so there is no `EmptyRef` equivalent here.
pub fn validate_selector(value: &ResourceSelector) -> Result<(), UnsupportedSelectorFieldError> {
    if value.did.is_some() {
        return Err(UnsupportedSelectorFieldError::Did);
    }
    Ok(())
}

/// Widens a ref into the selector that matches exactly it.
///
/// Not a `From` impl: the exact name has to be escaped into a wildcard-free
/// `LIKE` pattern, so this can silently widen the match if skipped. Naming it
/// keeps that visible at call sites.
pub fn ref_to_selector(value: ResourceRef) -> ResourceSelector {
    ResourceSelector {
        account: value.account,
        id: value.id,
        // A ref carrying a `did` is rejected by `validate_ref` before it can
        // reach here, so there is nothing to carry across.
        did: None,
        r#type: value.r#type,
        name: value
            .name
            .map(|name| sql_like_escape_literal(name.as_str())),
        labels: None,
    }
}

/// Escapes a literal so it matches only itself when used as a `LIKE` pattern.
///
/// Mirrors the escaping the repository applies to authored patterns, so
/// widening a ref into a selector cannot change which resources match.
fn sql_like_escape_literal(literal: &str) -> String {
    let mut escaped = String::with_capacity(literal.len());
    for ch in literal.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Coalescing
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `(schema, account)` — what makes two selectors mergeable.
type GroupKey = (Option<TypeUri>, Option<odf::AccountID>);

/// The same, as owned strings, so groups sort deterministically.
type GroupSortKey = (Option<String>, Option<String>);

/// One [`ResourceRef`] or [`ResourceSelector`] after its type has been resolved
/// to a concrete schema.
///
/// This is where the two converge, and it is deliberately *not* ODF-shaped: it
/// keeps `name` and `name_pattern` apart because the repository does. An exact
/// name becomes `ResourceQuery::ExactNames`, which several exact names share as
/// one row; a pattern becomes `ResourceQuery::NamePattern`, which cannot merge
/// with anything. A `ResourceRef` resolves into `name`, an authored selector
/// pattern into `name_pattern`.
///
/// `None` schema means the selector spans every type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSelector {
    pub schema: Option<TypeUri>,
    pub id: Option<ResourceID>,
    /// Exact name, from a [`ResourceRef`].
    pub name: Option<ResourceName>,
    /// SQL `LIKE` pattern, from a [`ResourceSelector`].
    pub name_pattern: Option<String>,
    /// `None` means the call-level account. Two selectors differing only by
    /// account must **not** merge, so this is part of the grouping key.
    pub account_id: Option<odf::AccountID>,
}

/// Folds scalar, ODF-shaped selectors into the list-carrying per-type queries
/// the repository speaks.
///
/// The wire is scalar to mirror the ODF spec; the repository keeps its
/// `ExactNames(Vec<_>)` / `ExactIds(Vec<_>)` lists so an N-name batch stays one
/// row and `FlatScope` arity stays proportional to types rather than names.
/// This function is the seam between the two.
///
/// Selectors are a logical OR, which maps onto the repository's per-row
/// `EXISTS` semantics. A type carrying more than one query *mode* therefore
/// emits one row per mode — [`ResourceTypeQuery`] holds a single
/// `Option<ResourceQuery>`, so `vs` with both an ID and a pattern cannot fold
/// into one row without changing meaning.
///
/// Returns `Ok(None)` when the input cannot match anything, so callers can skip
/// the query entirely. An empty input is vacuous, *not* "match everything" —
/// that is [`ResourceScope::AnyType(None)`], which a caller must ask for
/// explicitly with an unnarrowed type-less selector.
///
/// [`ResourceScope::AnyType(None)`]: kamu_resources::ResourceScope::AnyType
pub fn coalesce_selectors(
    selectors: Vec<ResolvedSelector>,
) -> Result<Option<ResourceScope>, UnrepresentableScopeError> {
    if selectors.is_empty() {
        return Ok(None);
    }

    // Checked before grouping, which merges selectors and loses the per-selector
    // identity this needs. One selector's fields are a *conjunction*, but the
    // repository's per-type rows are OR'd together, so a selector narrowing by
    // more than one mode cannot be expressed as rows without widening it.
    for selector in &selectors {
        let modes = usize::from(selector.id.is_some())
            + usize::from(selector.name.is_some())
            + usize::from(selector.name_pattern.is_some());
        if modes > 1 {
            return Err(UnrepresentableScopeError::SelectorNarrowsBySeveralModes);
        }
    }

    // Grouped by `(schema, account)`, preserving first-appearance order so the
    // emitted rows are stable and diffable. `None` schema (any-type) is its own
    // group.
    //
    // The account is part of the key because two selectors differing only by
    // account describe different resources: merging them would silently widen
    // one to the other's account.
    let mut order: Vec<GroupKey> = Vec::new();
    let mut groups: BTreeMap<GroupSortKey, SelectorGroup> = BTreeMap::new();

    for selector in selectors {
        let key = (selector.schema.clone(), selector.account_id.clone());
        let sort_key = (
            key.0.as_ref().map(|s| s.as_str().to_owned()),
            key.1.as_ref().map(ToString::to_string),
        );
        let group = groups.entry(sort_key).or_insert_with(|| {
            order.push(key);
            SelectorGroup::default()
        });
        group.push(selector);
    }

    // The any-type groups, across every account.
    let any_type_keys = order
        .iter()
        .filter(|(schema, _)| schema.is_none())
        .cloned()
        .collect::<Vec<_>>();

    if !any_type_keys.is_empty() {
        // Checked before anything else: an unnarrowed, account-less type-less
        // selector matches every resource under the call-level account, so it
        // subsumes every other selector rather than conflicting with them.
        let unnarrowed_any_account = any_type_keys.iter().any(|(_, account_id)| {
            account_id.is_none()
                && groups
                    .get(&(None, None))
                    .is_some_and(|group| group.unnarrowed)
        });
        if unnarrowed_any_account {
            return Ok(Some(ResourceScope::AnyType(None)));
        }

        // A type-less selector already spans every type, so pairing it with any
        // other group cannot be expressed as per-type rows.
        if order.len() > 1 {
            return Err(UnrepresentableScopeError::AnyTypeMixedWithTypedSelectors);
        }

        let (_, account_id) = &any_type_keys[0];

        // `AnyType` carries no per-row account, so one naming its own account
        // cannot be represented.
        if account_id.is_some() {
            return Err(UnrepresentableScopeError::AnyTypeWithAccount);
        }

        return groups[&(None, None)]
            .single_any_type_query()
            .map(|query| Some(ResourceScope::AnyType(Some(query))));
    }

    let mut type_queries = Vec::new();
    for (schema, account_id) in order {
        let sort_key = (
            schema.as_ref().map(|s| s.as_str().to_owned()),
            account_id.as_ref().map(ToString::to_string),
        );
        let Some(group) = groups.remove(&sort_key) else {
            continue;
        };
        group.emit_into(schema, account_id, &mut type_queries);
    }

    if type_queries.is_empty() {
        return Ok(None);
    }

    Ok(Some(ResourceScope::Types(type_queries)))
}

/// A selector combination the repository's scope shape cannot express.
///
/// Every variant is a limit of [`ResourceScope::AnyType`] carrying exactly one
/// query rather than a per-type list. They disappear once `AnyType` gives each
/// row its own type and account.
///
/// A fourth variant, `SingleTypeRequired`, was retired when `list` became the
/// multi-type `search`: rendering typed columns no longer forces one type.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum UnrepresentableScopeError {
    #[error(
        "A type-less selector cannot be combined with typed selectors in one call, because it \
         already spans every type"
    )]
    AnyTypeMixedWithTypedSelectors,

    /// `ResourceScope::AnyType` carries no per-row account, so a type-less
    /// selector cannot name one.
    #[error(
        "A type-less selector cannot name an account, because it spans every type under the \
         call-level account"
    )]
    AnyTypeWithAccount,

    #[error(
        "A type-less selector may narrow by only one of `id`, `name`, or `name` pattern at a time"
    )]
    AnyTypeMultipleQueryModes,

    /// Unlike the `AnyType*` variants, this one is not a limit of
    /// [`ResourceScope::AnyType`]: a selector's fields are a conjunction, while
    /// the repository's per-type rows are OR'd. Emitting one row per mode would
    /// widen the match rather than narrow it, so this survives the per-row-type
    /// stage.
    #[error("A selector may narrow by only one of `id` or `name` at a time")]
    SelectorNarrowsBySeveralModes,
}

/// The distinct query modes seen for one schema, deduplicated.
#[derive(Default)]
struct SelectorGroup {
    ids: Vec<ResourceID>,
    names: Vec<ResourceName>,
    patterns: Vec<String>,
    /// A selector that narrows by nothing matches the whole type, which
    /// subsumes every other mode in the group.
    unnarrowed: bool,
}

impl SelectorGroup {
    fn push(&mut self, selector: ResolvedSelector) {
        let ResolvedSelector {
            // Both are part of the group key, so every selector reaching one
            // group already agrees on them.
            schema: _,
            account_id: _,
            id,
            name,
            name_pattern,
        } = selector;

        if id.is_none() && name.is_none() && name_pattern.is_none() {
            self.unnarrowed = true;
            return;
        }

        // Duplicates would inflate pagination counts, so drop them here rather
        // than relying on the repository to deduplicate rows.
        if let Some(id) = id
            && !self.ids.contains(&id)
        {
            self.ids.push(id);
        }
        if let Some(name) = name
            && !self.names.contains(&name)
        {
            self.names.push(name);
        }
        if let Some(pattern) = name_pattern
            && !self.patterns.contains(&pattern)
        {
            self.patterns.push(pattern);
        }
    }

    /// The single query an `AnyType` scope can carry, or an error if this group
    /// needs more than one mode.
    fn single_any_type_query(&self) -> Result<ResourceQuery, UnrepresentableScopeError> {
        let modes = usize::from(!self.ids.is_empty())
            + usize::from(!self.names.is_empty())
            + self.patterns.len();
        if modes > 1 {
            return Err(UnrepresentableScopeError::AnyTypeMultipleQueryModes);
        }

        if !self.ids.is_empty() {
            return Ok(ResourceQuery::ExactIds(self.ids.clone()));
        }
        if !self.names.is_empty() {
            return Ok(ResourceQuery::ExactNames(self.names.clone()));
        }
        // `modes <= 1` and the group is narrowed, so exactly one pattern is
        // left; `unnarrowed` groups never reach here.
        Ok(ResourceQuery::NamePattern(self.patterns[0].clone()))
    }

    fn emit_into(
        self,
        schema: Option<TypeUri>,
        account_id: Option<odf::AccountID>,
        out: &mut Vec<ResourceTypeQuery>,
    ) {
        // Only reached for a concrete schema — any-type groups are resolved by
        // the caller, which owns the `AnyType` variant.
        let Some(schema) = schema else {
            return;
        };

        // An unnarrowed selector matches the whole type, subsuming every other
        // mode in the group.
        if self.unnarrowed {
            out.push(ResourceTypeQuery {
                schema,
                query: None,
                account_id,
            });
            return;
        }

        if !self.ids.is_empty() {
            out.push(ResourceTypeQuery {
                schema: schema.clone(),
                query: Some(ResourceQuery::ExactIds(self.ids)),
                account_id: account_id.clone(),
            });
        }
        if !self.names.is_empty() {
            out.push(ResourceTypeQuery {
                schema: schema.clone(),
                query: Some(ResourceQuery::ExactNames(self.names)),
                account_id: account_id.clone(),
            });
        }
        for pattern in self.patterns {
            out.push(ResourceTypeQuery {
                schema: schema.clone(),
                query: Some(ResourceQuery::NamePattern(pattern)),
                account_id: account_id.clone(),
            });
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use pretty_assertions::assert_eq;

    use super::*;

    const SCHEMA_A: &str = "https://example.org/A";
    const SCHEMA_B: &str = "https://example.org/B";

    fn schema(uri: &str) -> TypeUri {
        TypeUri::new_unchecked(uri)
    }

    fn name(s: &str) -> ResourceName {
        ResourceName::new_unchecked(s)
    }

    fn id(byte: u8) -> ResourceID {
        // Parsed rather than built from a `Uuid`, so the tests need no `uuid`
        // dependency of their own.
        format!("{byte:02x}{byte:02x}{byte:02x}{byte:02x}-0000-4000-8000-000000000000")
            .parse()
            .unwrap()
    }

    /// A selector for `uri` narrowed by nothing.
    fn any_of(uri: &str) -> ResolvedSelector {
        ResolvedSelector {
            schema: Some(schema(uri)),
            id: None,
            name: None,
            name_pattern: None,
            account_id: None,
        }
    }

    fn by_name(uri: &str, n: &str) -> ResolvedSelector {
        ResolvedSelector {
            name: Some(name(n)),
            ..any_of(uri)
        }
    }

    fn by_id(uri: &str, byte: u8) -> ResolvedSelector {
        ResolvedSelector {
            id: Some(id(byte)),
            ..any_of(uri)
        }
    }

    fn by_pattern(uri: &str, p: &str) -> ResolvedSelector {
        ResolvedSelector {
            name_pattern: Some(p.to_string()),
            ..any_of(uri)
        }
    }

    fn coalesce(selectors: Vec<ResolvedSelector>) -> Option<ResourceScope> {
        coalesce_selectors(selectors).unwrap()
    }

    fn types_of(scope: Option<ResourceScope>) -> Vec<ResourceTypeQuery> {
        match scope {
            Some(ResourceScope::Types(types)) => types,
            other => panic!("expected Types scope, got {other:?}"),
        }
    }

    // The `get_many` path: N exact names of one type must stay ONE row, since
    // `FlatScope` arity is per-type and the repo suite pins that pairing.
    #[test]
    fn test_many_names_of_one_type_fold_into_one_row() {
        let types = types_of(coalesce(vec![
            by_name(SCHEMA_A, "a"),
            by_name(SCHEMA_A, "b"),
            by_name(SCHEMA_A, "c"),
        ]));

        assert_eq!(types.len(), 1);
        assert_eq!(types[0].schema, schema(SCHEMA_A));
        assert_eq!(
            types[0].query,
            Some(ResourceQuery::ExactNames(vec![
                name("a"),
                name("b"),
                name("c")
            ]))
        );
    }

    #[test]
    fn test_many_ids_of_one_type_fold_into_one_row() {
        let types = types_of(coalesce(vec![by_id(SCHEMA_A, 1), by_id(SCHEMA_A, 2)]));

        assert_eq!(types.len(), 1);
        assert_eq!(
            types[0].query,
            Some(ResourceQuery::ExactIds(vec![id(1), id(2)]))
        );
    }

    // The case most likely to be got wrong: `ResourceTypeQuery` holds ONE
    // query, so mixed modes cannot fold into a single row without changing
    // meaning. They must fan out into one row per mode, relying on the repo's
    // per-row OR semantics.
    #[test]
    fn test_mixed_modes_within_one_type_emit_one_row_per_mode() {
        let types = types_of(coalesce(vec![
            by_id(SCHEMA_A, 1),
            by_name(SCHEMA_A, "a"),
            by_pattern(SCHEMA_A, "app-%"),
        ]));

        assert_eq!(types.len(), 3);
        assert!(types.iter().all(|t| t.schema == schema(SCHEMA_A)));
        assert_eq!(types[0].query, Some(ResourceQuery::ExactIds(vec![id(1)])));
        assert_eq!(
            types[1].query,
            Some(ResourceQuery::ExactNames(vec![name("a")]))
        );
        assert_eq!(
            types[2].query,
            Some(ResourceQuery::NamePattern("app-%".to_string()))
        );
    }

    // Two patterns cannot merge either — `LIKE` has no disjunction.
    #[test]
    fn test_multiple_patterns_of_one_type_emit_one_row_each() {
        let types = types_of(coalesce(vec![
            by_pattern(SCHEMA_A, "app-%"),
            by_pattern(SCHEMA_A, "%-prod"),
        ]));

        assert_eq!(types.len(), 2);
        assert_eq!(
            types[0].query,
            Some(ResourceQuery::NamePattern("app-%".to_string()))
        );
        assert_eq!(
            types[1].query,
            Some(ResourceQuery::NamePattern("%-prod".to_string()))
        );
    }

    // The headline capability: one call spanning two types, each with its own
    // query. Order must follow first appearance so rows stay diffable.
    #[test]
    fn test_multiple_types_keep_their_own_queries() {
        let types = types_of(coalesce(vec![
            by_pattern(SCHEMA_A, "app-%"),
            by_pattern(SCHEMA_B, "db-%"),
        ]));

        assert_eq!(types.len(), 2);
        assert_eq!(types[0].schema, schema(SCHEMA_A));
        assert_eq!(
            types[0].query,
            Some(ResourceQuery::NamePattern("app-%".to_string()))
        );
        assert_eq!(types[1].schema, schema(SCHEMA_B));
        assert_eq!(
            types[1].query,
            Some(ResourceQuery::NamePattern("db-%".to_string()))
        );
    }

    // An unnarrowed selector matches the whole type, so it subsumes the other
    // modes rather than adding a row alongside them.
    #[test]
    fn test_unnarrowed_selector_subsumes_narrower_ones_of_the_same_type() {
        let types = types_of(coalesce(vec![
            by_name(SCHEMA_A, "a"),
            any_of(SCHEMA_A),
            by_pattern(SCHEMA_A, "app-%"),
        ]));

        assert_eq!(types.len(), 1);
        assert_eq!(types[0].schema, schema(SCHEMA_A));
        assert_eq!(types[0].query, None);
    }

    #[test]
    fn test_duplicate_selectors_are_deduplicated() {
        // Duplicates would inflate pagination totals if they reached SQL.
        let types = types_of(coalesce(vec![
            by_name(SCHEMA_A, "a"),
            by_name(SCHEMA_A, "a"),
            by_id(SCHEMA_A, 1),
            by_id(SCHEMA_A, 1),
            by_pattern(SCHEMA_A, "app-%"),
            by_pattern(SCHEMA_A, "app-%"),
        ]));

        assert_eq!(types.len(), 3);
        assert_eq!(types[0].query, Some(ResourceQuery::ExactIds(vec![id(1)])));
        assert_eq!(
            types[1].query,
            Some(ResourceQuery::ExactNames(vec![name("a")]))
        );
        assert_eq!(
            types[2].query,
            Some(ResourceQuery::NamePattern("app-%".to_string()))
        );
    }

    // Empty input matches nothing. It must NOT degrade into "match everything",
    // which is the mistake that would silently widen every batch operation.
    #[test]
    fn test_empty_input_is_vacuous_not_match_everything() {
        assert_eq!(coalesce(vec![]), None);
    }

    // One selector's fields are a conjunction: `id=A AND name LIKE 'b-%'` means
    // "A, if it is also named b-something". Emitting an id row and a pattern row
    // would have the repository OR them, returning A *plus* every `b-%` resource
    // — a silently wider result than was asked for.
    #[test]
    fn test_one_selector_narrowing_by_id_and_pattern_is_rejected() {
        let result = coalesce_selectors(vec![ResolvedSelector {
            id: Some(id(1)),
            name_pattern: Some("b-%".to_string()),
            ..any_of(SCHEMA_A)
        }]);

        assert_matches!(
            result,
            Err(UnrepresentableScopeError::SelectorNarrowsBySeveralModes)
        );
    }

    #[test]
    fn test_type_less_unnarrowed_selector_is_any_type() {
        let scope = coalesce(vec![ResolvedSelector {
            schema: None,
            id: None,
            name: None,
            name_pattern: None,
            account_id: None,
        }]);

        assert_eq!(scope, Some(ResourceScope::AnyType(None)));
    }

    #[test]
    fn test_type_less_narrowed_selector_is_any_type_with_query() {
        let scope = coalesce(vec![ResolvedSelector {
            schema: None,
            id: None,
            name: Some(name("a")),
            name_pattern: None,
            account_id: None,
        }]);

        assert_eq!(
            scope,
            Some(ResourceScope::AnyType(Some(ResourceQuery::ExactNames(
                vec![name("a")]
            ))))
        );
    }

    // An unnarrowed any-type selector spans everything, so it wins outright
    // rather than erroring against its typed neighbours.
    #[test]
    fn test_unnarrowed_any_type_subsumes_typed_selectors() {
        let scope = coalesce(vec![
            by_pattern(SCHEMA_A, "app-%"),
            ResolvedSelector {
                schema: None,
                id: None,
                name: None,
                name_pattern: None,
                account_id: None,
            },
        ]);

        assert_eq!(scope, Some(ResourceScope::AnyType(None)));
    }

    // `ResourceScope::AnyType` carries a single query, so these two shapes are
    // genuinely unrepresentable today. Erroring beats silently dropping a
    // selector; stage 5 lifts the limitation.
    #[test]
    fn test_narrowed_any_type_mixed_with_typed_selectors_is_rejected() {
        let result = coalesce_selectors(vec![
            by_pattern(SCHEMA_A, "app-%"),
            ResolvedSelector {
                schema: None,
                id: None,
                name: Some(name("a")),
                name_pattern: None,
                account_id: None,
            },
        ]);

        assert_matches!(
            result,
            Err(UnrepresentableScopeError::AnyTypeMixedWithTypedSelectors)
        );
    }

    #[test]
    fn test_narrowed_any_type_with_multiple_query_modes_is_rejected() {
        let result = coalesce_selectors(vec![
            ResolvedSelector {
                schema: None,
                id: None,
                name: Some(name("a")),
                name_pattern: None,
                account_id: None,
            },
            ResolvedSelector {
                schema: None,
                id: None,
                name: None,
                name_pattern: Some("app-%".to_string()),
                account_id: None,
            },
        ]);

        assert_matches!(
            result,
            Err(UnrepresentableScopeError::AnyTypeMultipleQueryModes)
        );
    }

    ////////////////////////////////////////////////////////////////////////////
    // Refs and selectors
    ////////////////////////////////////////////////////////////////////////////

    fn type_ref(s: &str) -> kamu_resources::TypeRef {
        kamu_resources::TypeName::new_unchecked(s).into()
    }

    fn a_ref(name: Option<ResourceName>) -> ResourceRef {
        ResourceRef {
            account: None,
            r#type: Some(type_ref("SecretSet")),
            id: None,
            did: None,
            name,
        }
    }

    // `did` must fail loudly rather than be dropped — a caller who sets it
    // would otherwise get results for a filter that was never applied.
    #[test]
    fn test_ref_carrying_a_did_is_rejected() {
        let resource_ref = ResourceRef {
            did: Some(odf::metadata::Did::Odf(
                odf::metadata::DidOdf::new_seeded_ed25519(b"test"),
            )),
            ..a_ref(Some(name("my-secrets")))
        };

        assert_matches!(
            validate_ref(&resource_ref),
            Err(UnsupportedSelectorFieldError::Did)
        );
    }

    #[test]
    fn test_ref_naming_nothing_is_rejected() {
        assert_matches!(
            validate_ref(&a_ref(None)),
            Err(UnsupportedSelectorFieldError::EmptyRef)
        );
    }

    #[test]
    fn test_ref_naming_something_is_accepted() {
        assert_matches!(validate_ref(&a_ref(Some(name("my-secrets")))), Ok(()));
    }

    // A ref's name is exact, but a selector's is a `LIKE` pattern — so widening
    // must escape, or a name containing `%` would start matching its neighbours.
    #[test]
    fn test_widening_a_ref_escapes_wildcards_in_the_exact_name() {
        let selector = ref_to_selector(a_ref(Some(name("100%-done"))));

        assert_eq!(selector.name, Some(r"100\%-done".to_string()));
    }

    #[test]
    fn test_widening_a_ref_leaves_an_ordinary_name_alone() {
        let selector = ref_to_selector(a_ref(Some(name("my-secrets"))));

        assert_eq!(selector.name, Some("my-secrets".to_string()));
        assert_eq!(selector.r#type, Some(type_ref("SecretSet")));
    }

    // A type-less ref is resolved by searching every type, so it is accepted
    // here rather than rejected the way a `did` is.
    #[test]
    fn test_ref_without_a_type_is_accepted() {
        let resource_ref = ResourceRef {
            r#type: None,
            ..a_ref(Some(name("my-secrets")))
        };

        assert_matches!(validate_ref(&resource_ref), Ok(()));
        assert_eq!(ref_to_selector(resource_ref).r#type, None);
    }

    // The selector gained `did` when ODF made the type optional. Nothing can
    // resolve by it, so it must fail loudly rather than be silently ignored —
    // dropping it would return a *wider* result set than was asked for.
    #[test]
    fn test_selector_carrying_a_did_is_rejected() {
        let selector = ResourceSelector {
            did: Some(odf::metadata::Did::Odf(
                odf::metadata::DidOdf::new_seeded_ed25519(b"test"),
            )),
            ..ResourceSelector::of_type(type_ref("SecretSet"))
        };

        assert_matches!(
            validate_selector(&selector),
            Err(UnsupportedSelectorFieldError::Did)
        );
    }

    // Unlike a ref, a selector narrowing by nothing is meaningful: it matches
    // every resource of every type.
    #[test]
    fn test_selector_narrowing_by_nothing_is_accepted() {
        assert_matches!(validate_selector(&ResourceSelector::default()), Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
