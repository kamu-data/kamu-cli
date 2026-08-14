// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! ODF-shaped selector types for the resource facade.
//!
//! These mirror the ODF spec's split: a [`ResourceRef`] names exactly one
//! resource with an exact name, while a [`ResourceSelector`] matches zero or
//! many using a SQL `LIKE` pattern and label filters. Multiple selectors act as
//! a logical OR, which is what lets one call span several types and — from
//! stage 5 onwards — several accounts.
//!
//! Both are *structural twins* of the ODF types rather than the ODF types
//! themselves. They differ in exactly two ways:
//!
//! - the type position is a [`ResourceTypeSelectorRaw`] (`vs`, `variablesets`)
//!   rather than an ODF `TypeRef` (`VariableSet`). Those are different
//!   vocabularies: [`ResourceTypeDescriptor::matches_selector`] deliberately
//!   rejects the ODF type name, so a signature taking `TypeRef` would claim to
//!   accept `VariableSet` and then fail at resolution.
//! - `did` is absent. It is forward-reserved in ODF for the dataset/account
//!   conversion, and no repository can resolve by it today. The `TryFrom`
//!   conversions below reject a populated `did` as unsupported rather than
//!   dropping it silently.
//!
//! [`ResourceTypeDescriptor::matches_selector`]: kamu_resources::ResourceTypeDescriptor::matches_selector

use std::collections::BTreeMap;

use kamu_resources::{
    ResourceAccountRef,
    ResourceID,
    ResourceLabelFilterInput,
    ResourceName,
    ResourceQuery,
    ResourceScope,
    ResourceTypeQuery,
    ResourceTypeSelectorRaw,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Names exactly one resource by an exact name or an ID.
///
/// The ODF `ResourceRef` analogue. Use [`ResourceSelector`] for anything that
/// may match more than one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRef {
    pub account: Option<ResourceAccountRef>,
    /// Required for a ref — an exact name is only unique within a type.
    pub resource_type: ResourceTypeSelectorRaw,
    pub id: Option<ResourceID>,
    /// Exact — never a pattern.
    pub name: Option<ResourceName>,
}

impl ResourceRef {
    /// Rejects a ref that names nothing, mirroring how an empty account
    /// selector is rejected at the manifest boundary.
    pub fn validate(&self) -> Result<(), EmptyResourceRefError> {
        if self.id.is_none() && self.name.is_none() {
            return Err(EmptyResourceRefError);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Resource reference must specify at least one of `id` or `name`")]
pub struct EmptyResourceRefError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Matches zero or many resources using identity and label filters.
///
/// The ODF `ResourceSelector` analogue, with one documented superset: a `None`
/// `resource_type` means *any type*. The spec requires `type`, but the API
/// already supports type-less listing, and encoding that as `None` avoids a
/// magic `%` token on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResourceSelector {
    pub account: Option<ResourceAccountRef>,
    /// `None` matches every registered resource type.
    pub resource_type: Option<ResourceTypeSelectorRaw>,
    pub id: Option<ResourceID>,
    /// Exact name. Kept separate from `name_pattern` because the two resolve to
    /// different repository queries and carry different not-found semantics.
    pub name: Option<ResourceName>,
    /// SQL `LIKE` pattern. A `String`, not a [`ResourceName`], since wildcards
    /// are not valid in a name.
    pub name_pattern: Option<String>,
    pub labels: Option<ResourceLabelFilterInput>,
}

impl From<ResourceRef> for ResourceSelector {
    fn from(value: ResourceRef) -> Self {
        Self {
            account: value.account,
            resource_type: Some(value.resource_type),
            id: value.id,
            name: value.name,
            name_pattern: None,
            labels: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ODF conversions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A field that ODF defines but the facade cannot honour yet.
#[derive(Debug, thiserror::Error)]
pub enum UnsupportedOdfSelectorFieldError {
    /// `did` is forward-reserved in ODF for when datasets and accounts become
    /// resources. Rejected rather than ignored so a caller who sets it learns
    /// it had no effect.
    #[error("Resource `did` is reserved for future use and cannot be resolved yet")]
    Did,
}

impl TryFrom<odf::metadata::resource::ResourceRef> for ResourceRef {
    type Error = UnsupportedOdfSelectorFieldError;

    fn try_from(value: odf::metadata::resource::ResourceRef) -> Result<Self, Self::Error> {
        let odf::metadata::resource::ResourceRef {
            account,
            r#type,
            id,
            did,
            name,
        } = value;

        if did.is_some() {
            return Err(UnsupportedOdfSelectorFieldError::Did);
        }

        Ok(Self {
            account,
            resource_type: type_ref_to_raw_selector(&r#type),
            id,
            name,
        })
    }
}

impl TryFrom<odf::metadata::resource::ResourceSelector> for ResourceSelector {
    type Error = UnsupportedOdfSelectorFieldError;

    fn try_from(value: odf::metadata::resource::ResourceSelector) -> Result<Self, Self::Error> {
        let odf::metadata::resource::ResourceSelector {
            account,
            r#type,
            id,
            name,
            labels,
        } = value;

        Ok(Self {
            account,
            resource_type: Some(type_ref_to_raw_selector(&r#type)),
            id,
            // ODF's `name` is always a `LIKE` pattern, even when it happens to
            // contain no wildcard.
            name: None,
            name_pattern: name,
            labels,
        })
    }
}

/// ODF spells a type as `VariableSet`; the facade resolves whatever the user
/// typed against canonical selectors and aliases. Passing the name through
/// unchanged is correct only because [`ResourceTypeSelectorRaw`] accepts any
/// non-empty trimmed string — resolution happens later, against descriptors.
fn type_ref_to_raw_selector(
    type_ref: &odf::metadata::resource::TypeRef,
) -> ResourceTypeSelectorRaw {
    ResourceTypeSelectorRaw::new_unchecked(type_ref.as_str())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Coalescing
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One selector after its type has been resolved to a concrete schema.
///
/// `None` schema means the selector spans every type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSelector {
    pub schema: Option<TypeUri>,
    pub id: Option<ResourceID>,
    pub name: Option<ResourceName>,
    pub name_pattern: Option<String>,
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

    // Grouped by schema, preserving first-appearance order so the emitted rows
    // are stable and diffable. `None` (any-type) is its own group.
    let mut order: Vec<Option<TypeUri>> = Vec::new();
    let mut groups: BTreeMap<Option<String>, SelectorGroup> = BTreeMap::new();

    for selector in selectors {
        let key = selector.schema.as_ref().map(|s| s.as_str().to_owned());
        let group = groups.entry(key).or_insert_with(|| {
            order.push(selector.schema.clone());
            SelectorGroup::default()
        });
        group.push(selector);
    }

    // An unnarrowed any-type selector matches everything, so it subsumes every
    // other selector in the call and collapses the whole scope.
    let any_type_group = groups.get(&None);
    if any_type_group.is_some_and(|group| group.unnarrowed) {
        return Ok(Some(ResourceScope::AnyType(None)));
    }

    // A narrowed any-type selector spans every type, so it cannot be expressed
    // as a per-type row. `ResourceScope::AnyType` carries a single query, so it
    // is representable only as the sole selector with a single query mode.
    if let Some(group) = any_type_group {
        if order.len() > 1 {
            return Err(UnrepresentableScopeError::AnyTypeMixedWithTypedSelectors);
        }
        return group
            .single_any_type_query()
            .map(|query| Some(ResourceScope::AnyType(Some(query))));
    }

    let mut type_queries = Vec::new();
    for schema in order {
        let key = schema.as_ref().map(|s| s.as_str().to_owned());
        let Some(group) = groups.remove(&key) else {
            continue;
        };
        group.emit_into(schema, &mut type_queries);
    }

    if type_queries.is_empty() {
        return Ok(None);
    }

    Ok(Some(ResourceScope::Types(type_queries)))
}

/// A selector combination the repository's scope shape cannot express.
///
/// Both variants are limits of [`ResourceScope::AnyType`] carrying exactly one
/// query rather than a per-type list. Stage 5 removes them by giving every row
/// its own account and type.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum UnrepresentableScopeError {
    #[error(
        "A type-less selector cannot be combined with typed selectors in one call, because it \
         already spans every type"
    )]
    AnyTypeMixedWithTypedSelectors,

    #[error(
        "A type-less selector may narrow by only one of `id`, `name`, or `name` pattern at a time"
    )]
    AnyTypeMultipleQueryModes,
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
            schema: _,
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

    fn emit_into(self, schema: Option<TypeUri>, out: &mut Vec<ResourceTypeQuery>) {
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
            });
            return;
        }

        if !self.ids.is_empty() {
            out.push(ResourceTypeQuery {
                schema: schema.clone(),
                query: Some(ResourceQuery::ExactIds(self.ids)),
            });
        }
        if !self.names.is_empty() {
            out.push(ResourceTypeQuery {
                schema: schema.clone(),
                query: Some(ResourceQuery::ExactNames(self.names)),
            });
        }
        for pattern in self.patterns {
            out.push(ResourceTypeQuery {
                schema: schema.clone(),
                query: Some(ResourceQuery::NamePattern(pattern)),
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

    #[test]
    fn test_type_less_unnarrowed_selector_is_any_type() {
        let scope = coalesce(vec![ResolvedSelector {
            schema: None,
            id: None,
            name: None,
            name_pattern: None,
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
            },
            ResolvedSelector {
                schema: None,
                id: None,
                name: None,
                name_pattern: Some("app-%".to_string()),
            },
        ]);

        assert_matches!(
            result,
            Err(UnrepresentableScopeError::AnyTypeMultipleQueryModes)
        );
    }

    ////////////////////////////////////////////////////////////////////////////
    // ODF conversions
    ////////////////////////////////////////////////////////////////////////////

    fn type_ref(s: &str) -> odf::metadata::resource::TypeRef {
        odf::metadata::resource::TypeName::new_unchecked(s).into()
    }

    #[test]
    fn test_odf_ref_converts_with_exact_name() {
        let odf_ref = odf::metadata::resource::ResourceRef {
            account: None,
            r#type: type_ref("SecretSet"),
            id: None,
            did: None,
            name: Some(name("my-secrets")),
        };

        let converted = ResourceRef::try_from(odf_ref).unwrap();

        assert_eq!(converted.resource_type.as_str(), "SecretSet");
        assert_eq!(converted.name, Some(name("my-secrets")));
        assert_eq!(converted.id, None);
    }

    // `did` must fail loudly rather than be dropped — a caller who sets it
    // would otherwise get results for a filter that was never applied.
    #[test]
    fn test_odf_ref_rejects_populated_did() {
        let odf_ref = odf::metadata::resource::ResourceRef {
            account: None,
            r#type: type_ref("SecretSet"),
            id: None,
            did: Some(odf::metadata::Did::Odf(
                odf::metadata::DidOdf::new_seeded_ed25519(b"test"),
            )),
            name: Some(name("my-secrets")),
        };

        assert_matches!(
            ResourceRef::try_from(odf_ref),
            Err(UnsupportedOdfSelectorFieldError::Did)
        );
    }

    // ODF's selector `name` is a LIKE pattern by definition, so it must land in
    // `name_pattern` — putting it in `name` would turn a pattern into an exact
    // lookup and silently return nothing.
    #[test]
    fn test_odf_selector_name_becomes_a_pattern_not_an_exact_name() {
        let odf_selector = odf::metadata::resource::ResourceSelector {
            account: None,
            r#type: type_ref("SecretSet"),
            id: None,
            name: Some("app-%".to_string()),
            labels: None,
        };

        let converted = ResourceSelector::try_from(odf_selector).unwrap();

        assert_eq!(converted.name_pattern, Some("app-%".to_string()));
        assert_eq!(converted.name, None);
        assert_eq!(
            converted.resource_type.map(|t| t.as_str().to_string()),
            Some("SecretSet".to_string())
        );
    }

    #[test]
    fn test_facade_ref_widens_into_a_selector() {
        let resource_ref = ResourceRef {
            account: None,
            resource_type: ResourceTypeSelectorRaw::new_unchecked("vs"),
            id: None,
            name: Some(name("my-vars")),
        };

        let selector = ResourceSelector::from(resource_ref);

        // The exact name must stay exact when widening.
        assert_eq!(selector.name, Some(name("my-vars")));
        assert_eq!(selector.name_pattern, None);
    }

    #[test]
    fn test_ref_naming_nothing_is_rejected() {
        let resource_ref = ResourceRef {
            account: None,
            resource_type: ResourceTypeSelectorRaw::new_unchecked("vs"),
            id: None,
            name: None,
        };

        assert_matches!(resource_ref.validate(), Err(EmptyResourceRefError));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
