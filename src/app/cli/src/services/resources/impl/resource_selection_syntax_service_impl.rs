// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::sync::Arc;

use kamu_resources::ResourceTypeDescriptor;

use super::resource_selection_syntax_parser::{
    ALL_SELECTOR,
    ANY_SELECTOR,
    ParsedSyntax,
    ResourceSelectionSyntaxParser,
};
use crate::CLIError;
use crate::resources::{
    ResolvedResourceSelector,
    ResourceExactSelector,
    ResourceSelectionItem,
    ResourceSelectionSyntax,
    ResourceSelectionSyntaxService,
    ResourceSelectorResolutionService,
    ResourceShadowedSelector,
    ResourceTypeLookupErrorOptions,
    ResourceTypeLookupService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const UNSUPPORTED_TYPE_TARGET_PREFIX: &str = "Unsupported get target";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The type part of a selector, once classified.
#[derive(Debug, Clone)]
enum ResourceTypeToken {
    /// `%` — spans every supported type.
    AnyType,
    /// An exact canonical selector name or alias.
    Exact(Box<ResourceTypeDescriptor>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceSelectionSyntaxService)]
pub struct ResourceSelectionSyntaxServiceImpl {
    resource_type_lookup_service: Arc<dyn ResourceTypeLookupService>,
    resource_selector_resolution_service: Arc<dyn ResourceSelectorResolutionService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceSelectionSyntaxService for ResourceSelectionSyntaxServiceImpl {
    async fn parse_get_args(
        &self,
        explicit_context_name: Option<&str>,
        args: &[String],
    ) -> Result<ResourceSelectionSyntax, CLIError> {
        let parsed = ResourceSelectionSyntaxParser::parse(args)?;

        if let ParsedSyntax::ById { input } = parsed {
            let resolved = self
                .resource_selector_resolution_service
                .resolve_single_selector(input)
                .await?;

            return Ok(Self::by_id_syntax(resolved));
        }

        let supported_resource_types = self
            .resource_type_lookup_service
            .list_supported_resource_types(explicit_context_name)
            .await?;

        Self::interpret_parsed_syntax_with(
            &supported_resource_types,
            parsed,
            |type_str, selector_input| {
                self.make_selector_item(&supported_resource_types, type_str, selector_input)
            },
        )
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSelectionSyntaxServiceImpl {
    async fn interpret_parsed_syntax_with<F, Fut>(
        supported_resource_types: &[ResourceTypeDescriptor],
        parsed: ParsedSyntax<'_>,
        mut make_selector_item: F,
    ) -> Result<ResourceSelectionSyntax, CLIError>
    where
        F: FnMut(String, String) -> Fut,
        Fut: Future<Output = Result<ResourceSelectionItem, CLIError>>,
    {
        let mut items = Vec::new();
        let mut shadowed_selectors = Vec::new();

        match parsed {
            // Handled directly in `parse_get_args` before reaching here.
            ParsedSyntax::ById { .. } => unreachable!("ById is handled in parse_get_args"),

            ParsedSyntax::All { shadowed_inputs } => {
                items.push(ResourceSelectionItem::All);

                // Shadowed selectors are reported separately instead of being kept in
                // `items`, so downstream resolution never wastes backend lookups on
                // selectors whose scope is already covered by `all`.
                for shadowed_input in shadowed_inputs {
                    if let Some(type_str) = shadowed_input.type_str {
                        Self::validate_shadowed_resource_type_target(
                            supported_resource_types,
                            type_str,
                        )?;
                    }
                    shadowed_selectors.push(ResourceShadowedSelector {
                        selector_input: shadowed_input.display.to_owned(),
                    });
                }
            }

            // type sel1 sel2 ..
            ParsedSyntax::SameType {
                type_str,
                selector_inputs,
            } => {
                // Same-type syntax has a single explicit type and only varies by
                // selector names. The only broad token in this mode is `all`,
                // so coverage checks are O(n) and local to the selector list.

                // Note: parsing ensured `type_str` is never "all", that goes to
                // `ParsedSyntax::All`. `type_str` can still be `%` (all types).

                if selector_inputs
                    .iter()
                    .any(|selector_input| selector_input.eq_ignore_ascii_case(ALL_SELECTOR))
                {
                    // Keep the original `all` spelling as typed by user in
                    // `selector_input` for diagnostics output.
                    items.push(Self::make_all_by_type_item(
                        supported_resource_types,
                        type_str,
                        selector_inputs
                            .iter()
                            .find(|selector_input| {
                                selector_input.eq_ignore_ascii_case(ALL_SELECTOR)
                            })
                            .expect("same-type all selector should exist")
                            .to_string(),
                    )?);

                    // Any concrete selector for the same type is fully covered
                    // by `<type>/all`, so record it as shadowed instead of
                    // scheduling additional selector-resolution work.
                    for selector_input in selector_inputs {
                        if !selector_input.eq_ignore_ascii_case(ALL_SELECTOR) {
                            // Keep only the broad selector in `items`; narrower selectors
                            // move to `shadowed_selectors` so resolution and delete
                            // validation can treat them as already covered.
                            shadowed_selectors.push(ResourceShadowedSelector {
                                selector_input: selector_input.to_owned(),
                            });
                        }
                    }
                } else {
                    for selector_input in selector_inputs {
                        items.push(
                            make_selector_item(type_str.to_owned(), selector_input.to_owned())
                                .await?,
                        );
                    }
                }
            }

            // type1/sel1 type2/sel2 ..
            ParsedSyntax::RefForm { pairs } => {
                // Broad selector coverage is either one type or `%` (all of
                // them), so superset checks are direct comparisons.
                let mut broad_selector_coverages: Vec<TypeCoverage> = Vec::new();
                let mut broad_selector_union_coverage = TypeCoverageUnion::default();
                for (type_str, selector_input) in &pairs {
                    if let Some(coverage) = Self::ref_form_broad_resource_type_coverage(
                        supported_resource_types,
                        type_str,
                        selector_input,
                    ) {
                        broad_selector_union_coverage.union_with(coverage);
                        Self::push_unique_coverage(&mut broad_selector_coverages, coverage);
                    }
                }

                // Avoid emitting duplicate broad selectors that expand to exactly
                // the same set of target types (e.g. aliases), while still
                // reporting shadowed specifics.
                let mut emitted_broad_selectors: Vec<TypeCoverage> = Vec::new();

                for (type_str, selector_input) in pairs {
                    let selector_display = format!("{type_str}/{selector_input}");
                    // Compute the type set this selector can possibly touch.
                    // If type lookup fails, treat it as a regular selector and
                    // defer error handling to item construction.
                    let Some(current_selector_matched_types) =
                        Self::resource_type_coverage_for_selector_target(
                            supported_resource_types,
                            type_str,
                        )
                    else {
                        items.push(
                            make_selector_item(type_str.to_owned(), selector_input.to_owned())
                                .await?,
                        );
                        continue;
                    };

                    let is_broad_selector = Self::is_ref_form_broad_selector(selector_input);
                    // Broad selector is shadowed when some other broad selector
                    // covers a strict superset of its matched types.
                    let is_shadowed_by_broader_selector =
                        broad_selector_coverages.iter().any(|broad_coverage| {
                            *broad_coverage != current_selector_matched_types
                                && broad_coverage.is_superset(current_selector_matched_types)
                        });

                    if is_shadowed_by_broader_selector {
                        shadowed_selectors.push(ResourceShadowedSelector {
                            selector_input: selector_display,
                        });
                        continue;
                    }

                    if is_broad_selector {
                        // Emit only once per covered type-set to keep `items`
                        // minimal and deterministic.
                        if Self::push_unique_coverage(
                            &mut emitted_broad_selectors,
                            current_selector_matched_types,
                        ) {
                            items.push(Self::make_all_by_type_item(
                                supported_resource_types,
                                type_str,
                                selector_input.to_owned(),
                            )?);
                        }
                    } else if broad_selector_union_coverage
                        .is_superset(current_selector_matched_types)
                    {
                        // Narrow selector in ref-form is shadowed when its
                        // matched type-set is contained in the union of broad
                        // selector coverages.
                        shadowed_selectors.push(ResourceShadowedSelector {
                            selector_input: selector_display,
                        });
                    } else {
                        items.push(
                            make_selector_item(type_str.to_owned(), selector_input.to_owned())
                                .await?,
                        );
                    }
                }
            }
        }

        Ok(ResourceSelectionSyntax {
            items,
            shadowed_selectors,
        })
    }

    /// Only *names* may carry `%` wildcards.
    fn is_name_pattern(input: &str) -> bool {
        input.contains('%')
    }

    /// The type part is matched exactly, with `%` as the sole special token
    /// meaning "all types". Any other `%` spelling (`%set`, `s%`) is rejected:
    /// wildcard type matching is hard to read back and dangerous on `delete`.
    fn classify_type_token(
        supported_resource_types: &[ResourceTypeDescriptor],
        type_str: &str,
    ) -> Result<ResourceTypeToken, CLIError> {
        if type_str == ANY_SELECTOR {
            return Ok(ResourceTypeToken::AnyType);
        }

        if Self::is_name_pattern(type_str) {
            return Err(Self::unsupported_type_target_error(
                supported_resource_types,
                type_str,
            ));
        }

        let type_descriptor = Self::resolve_type_descriptor(
            supported_resource_types,
            type_str,
            &ResourceTypeLookupErrorOptions::new(UNSUPPORTED_TYPE_TARGET_PREFIX),
        )?;

        Ok(ResourceTypeToken::Exact(Box::new(type_descriptor)))
    }

    fn unsupported_type_target_error(
        supported_resource_types: &[ResourceTypeDescriptor],
        type_str: &str,
    ) -> CLIError {
        CLIError::usage_error(format!(
            "{UNSUPPORTED_TYPE_TARGET_PREFIX} '{type_str}'. Supported targets: {}",
            Self::supported_targets(supported_resource_types, &[]).join(", ")
        ))
    }

    fn by_id_syntax(resolved: ResolvedResourceSelector) -> ResourceSelectionSyntax {
        ResourceSelectionSyntax {
            items: vec![ResourceSelectionItem::ExactAnyType {
                selector_input: resolved.input,
                resource_ref: resolved.resource_ref,
            }],
            shadowed_selectors: Vec::new(),
        }
    }

    /// Builds the item for a broad (`all` / `%`) name selector on `type_str`.
    fn make_all_by_type_item(
        supported_resource_types: &[ResourceTypeDescriptor],
        type_str: &str,
        selector_input: String,
    ) -> Result<ResourceSelectionItem, CLIError> {
        match Self::classify_type_token(supported_resource_types, type_str)? {
            // Every type, every name — indistinguishable from a plain `all`.
            ResourceTypeToken::AnyType => Ok(ResourceSelectionItem::All),
            ResourceTypeToken::Exact(type_descriptor) => Ok(ResourceSelectionItem::AllByType {
                type_descriptor: *type_descriptor,
                selector_input,
            }),
        }
    }

    fn validate_shadowed_resource_type_target(
        supported_resource_types: &[ResourceTypeDescriptor],
        type_str: &str,
    ) -> Result<(), CLIError> {
        Self::classify_type_token(supported_resource_types, type_str)?;

        Ok(())
    }

    async fn make_selector_item(
        &self,
        supported_resource_types: &[ResourceTypeDescriptor],
        type_str: String,
        selector_input: String,
    ) -> Result<ResourceSelectionItem, CLIError> {
        Self::make_selector_item_with(
            supported_resource_types,
            type_str,
            selector_input,
            |selector_input| async move {
                self.resource_selector_resolution_service
                    .resolve_single_selector(selector_input.as_str())
                    .await
            },
        )
        .await
    }

    async fn make_selector_item_with<F, Fut>(
        supported_resource_types: &[ResourceTypeDescriptor],
        type_str: String,
        selector_input: String,
        resolve_selector: F,
    ) -> Result<ResourceSelectionItem, CLIError>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<ResolvedResourceSelector, CLIError>>,
    {
        let type_token = Self::classify_type_token(supported_resource_types, type_str.as_str())?;

        match (type_token, Self::is_name_pattern(selector_input.as_str())) {
            (ResourceTypeToken::Exact(type_descriptor), false) => {
                let resolved = resolve_selector(selector_input).await?;

                Ok(ResourceSelectionItem::Exact(ResourceExactSelector {
                    type_descriptor: *type_descriptor,
                    selector_input: resolved.input,
                    resource_ref: resolved.resource_ref,
                }))
            }
            (ResourceTypeToken::Exact(type_descriptor), true) => {
                Ok(ResourceSelectionItem::NamePattern {
                    type_descriptor: *type_descriptor,
                    selector_input: selector_input.clone(),
                    name_pattern: selector_input,
                })
            }
            (ResourceTypeToken::AnyType, false) => {
                let resolved = resolve_selector(selector_input).await?;

                Ok(ResourceSelectionItem::AnyTypeExactRef {
                    selector_input: format!("{type_str}/{}", resolved.input),
                    resource_ref: resolved.resource_ref,
                })
            }
            (ResourceTypeToken::AnyType, true) => Ok(ResourceSelectionItem::AnyTypeNamePattern {
                selector_input: format!("{type_str}/{selector_input}"),
                name_pattern: selector_input,
            }),
        }
    }

    fn resolve_type_descriptor(
        supported_resource_types: &[ResourceTypeDescriptor],
        target: &str,
        error_options: &ResourceTypeLookupErrorOptions,
    ) -> Result<ResourceTypeDescriptor, CLIError> {
        supported_resource_types
            .iter()
            .find(|descriptor| descriptor.matches_selector(target))
            .cloned()
            .ok_or_else(|| {
                CLIError::usage_error(format!(
                    "{} '{target}'. Supported targets: {}",
                    error_options.unsupported_prefix,
                    Self::supported_targets(
                        supported_resource_types,
                        &error_options.additional_targets
                    )
                    .join(", ")
                ))
            })
    }

    fn supported_targets(
        supported_resource_types: &[ResourceTypeDescriptor],
        additional_targets: &[String],
    ) -> Vec<String> {
        let mut targets = additional_targets.to_vec();

        for descriptor in supported_resource_types {
            targets.push(descriptor.canonical_selector.to_string());
            targets.extend(descriptor.selector_aliases.iter().map(ToString::to_string));
        }

        targets.sort();
        targets.dedup();
        targets
    }

    /// A broad *name* selector: `all` or `%`, both meaning "every name".
    fn is_ref_form_broad_selector(selector_input: &str) -> bool {
        selector_input.eq_ignore_ascii_case(ALL_SELECTOR) || selector_input == ANY_SELECTOR
    }

    fn ref_form_broad_resource_type_coverage(
        supported_resource_types: &[ResourceTypeDescriptor],
        type_str: &str,
        selector_input: &str,
    ) -> Option<TypeCoverage> {
        Self::is_ref_form_broad_selector(selector_input)
            .then(|| {
                Self::resource_type_coverage_for_selector_target(supported_resource_types, type_str)
            })
            .flatten()
    }

    fn push_unique_coverage(coverages: &mut Vec<TypeCoverage>, coverage: TypeCoverage) -> bool {
        if coverages.contains(&coverage) {
            false
        } else {
            coverages.push(coverage);
            true
        }
    }

    /// The set of types a selector target can touch, or `None` when the target
    /// resolves to no known type.
    fn resource_type_coverage_for_selector_target(
        supported_resource_types: &[ResourceTypeDescriptor],
        type_str: &str,
    ) -> Option<TypeCoverage> {
        if type_str == ANY_SELECTOR {
            return Some(TypeCoverage::AllTypes);
        }

        supported_resource_types
            .iter()
            .position(|descriptor| descriptor.matches_selector(type_str))
            .map(TypeCoverage::OneType)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Which types a selector covers. Coverage never partially overlaps: a selector
/// names exactly one type or, via `%`, all of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeCoverage {
    AllTypes,
    OneType(usize),
}

impl TypeCoverage {
    fn is_superset(self, other: Self) -> bool {
        match (self, other) {
            (Self::AllTypes, _) => true,
            (Self::OneType(_), Self::AllTypes) => false,
            (Self::OneType(lhs), Self::OneType(rhs)) => lhs == rhs,
        }
    }
}

/// The union of the broad selectors seen so far.
#[derive(Debug, Default)]
struct TypeCoverageUnion {
    all_types: bool,
    types: Vec<usize>,
}

impl TypeCoverageUnion {
    fn union_with(&mut self, coverage: TypeCoverage) {
        match coverage {
            TypeCoverage::AllTypes => self.all_types = true,
            TypeCoverage::OneType(index) => {
                if !self.types.contains(&index) {
                    self.types.push(index);
                }
            }
        }
    }

    fn is_superset(&self, coverage: TypeCoverage) -> bool {
        if self.all_types {
            return true;
        }

        match coverage {
            TypeCoverage::AllTypes => false,
            TypeCoverage::OneType(index) => self.types.contains(&index),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use kamu_resources::TypeUri;
    use kamu_resources_facade::ResourceRef;
    use pretty_assertions::assert_eq;

    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(ToString::to_string).collect()
    }

    fn supported_resource_types() -> Vec<ResourceTypeDescriptor> {
        vec![
            ResourceTypeDescriptor {
                canonical_selector: kamu_resources::ResourceSelectorName::new("variables").unwrap(),
                selector_aliases: vec![kamu_resources::ResourceSelectorName::new("vs").unwrap()],
                schema: TypeUri::new_unchecked("dev.kamu/variable/v1"),
                list_columns: Vec::new(),
            },
            ResourceTypeDescriptor {
                canonical_selector: kamu_resources::ResourceSelectorName::new("secrets").unwrap(),
                selector_aliases: vec![kamu_resources::ResourceSelectorName::new("ss").unwrap()],
                schema: TypeUri::new_unchecked("dev.kamu/secret/v1"),
                list_columns: Vec::new(),
            },
        ]
    }

    fn resolved_selector(input: &str) -> ResolvedResourceSelector {
        ResolvedResourceSelector {
            input: input.to_owned(),
            resource_ref: ResourceRef::ByName(input.parse().unwrap()),
        }
    }

    async fn try_interpret(selector_args: &[&str]) -> Result<ResourceSelectionSyntax, CLIError> {
        let selector_args = args(selector_args);
        let parsed = ResourceSelectionSyntaxParser::parse(&selector_args)?;
        let supported_resource_types = supported_resource_types();

        ResourceSelectionSyntaxServiceImpl::interpret_parsed_syntax_with(
            &supported_resource_types,
            parsed,
            |type_str, selector_input| {
                let supported_resource_types = supported_resource_types.clone();
                async move {
                    ResourceSelectionSyntaxServiceImpl::make_selector_item_with(
                        &supported_resource_types,
                        type_str,
                        selector_input,
                        |selector_input| async move { Ok(resolved_selector(&selector_input)) },
                    )
                    .await
                }
            },
        )
        .await
    }

    async fn interpret(selector_args: &[&str]) -> ResourceSelectionSyntax {
        try_interpret(selector_args).await.unwrap()
    }

    fn describe_item(item: &ResourceSelectionItem) -> String {
        match item {
            ResourceSelectionItem::All => "all".to_owned(),
            ResourceSelectionItem::AllByType {
                type_descriptor,
                selector_input,
            } => format!(
                "all-by-type:{}:{selector_input}",
                type_descriptor.canonical_selector
            ),
            ResourceSelectionItem::Exact(ResourceExactSelector {
                type_descriptor,
                selector_input,
                ..
            }) => format!(
                "exact:{}:{selector_input}",
                type_descriptor.canonical_selector
            ),
            ResourceSelectionItem::ExactAnyType { selector_input, .. } => {
                format!("exact-any-type:{selector_input}")
            }
            ResourceSelectionItem::NamePattern {
                type_descriptor,
                selector_input,
                ..
            } => format!(
                "name-pattern:{}:{selector_input}",
                type_descriptor.canonical_selector
            ),
            ResourceSelectionItem::AnyTypeExactRef { selector_input, .. } => {
                format!("any-type-exact:{selector_input}")
            }
            ResourceSelectionItem::AnyTypeNamePattern { selector_input, .. } => {
                format!("any-type-name:{selector_input}")
            }
        }
    }

    fn describe_shadowed(syntax: &ResourceSelectionSyntax) -> Vec<String> {
        syntax
            .shadowed_selectors
            .iter()
            .map(|selector| selector.selector_input.clone())
            .collect()
    }

    fn describe_items(syntax: &ResourceSelectionSyntax) -> Vec<String> {
        syntax.items.iter().map(describe_item).collect()
    }

    #[tokio::test]
    async fn test_interpret_ref_form_broad_selector_matrix() {
        struct Case {
            name: &'static str,
            input: &'static [&'static str],
            expected_items: &'static [&'static str],
            expected_shadowed: &'static [&'static str],
        }

        let cases = [
            Case {
                name: "global broad shadows exact type broad",
                input: &["%/all", "ss/%"],
                expected_items: &["all"],
                expected_shadowed: &["ss/%"],
            },
            Case {
                name: "equal broad coverages emit once without shadowing",
                input: &["ss/%", "secrets/all"],
                expected_items: &["all-by-type:secrets:%"],
                expected_shadowed: &[],
            },
            Case {
                name: "duplicate alias broad coverage emits once",
                input: &["secrets/%", "ss/%"],
                expected_items: &["all-by-type:secrets:%"],
                expected_shadowed: &[],
            },
            Case {
                name: "global wildcard normalizes to all",
                input: &["%/%"],
                expected_items: &["all"],
                expected_shadowed: &[],
            },
            Case {
                name: "global broad shadows every exact type broad",
                input: &["%/%", "ss/%", "vs/all"],
                expected_items: &["all"],
                expected_shadowed: &["ss/%", "vs/all"],
            },
        ];

        for case in cases {
            let syntax = interpret(case.input).await;

            assert_eq!(
                describe_items(&syntax),
                case.expected_items,
                "items mismatch for case: {}",
                case.name,
            );
            assert_eq!(
                describe_shadowed(&syntax),
                case.expected_shadowed,
                "shadowed mismatch for case: {}",
                case.name,
            );
        }
    }

    #[tokio::test]
    async fn test_interpret_ref_form_narrow_selector_shadow_matrix() {
        struct Case {
            name: &'static str,
            input: &'static [&'static str],
            expected_items: &'static [&'static str],
            expected_shadowed: &'static [&'static str],
        }

        let cases = [
            Case {
                name: "global broad shadows exact selector",
                input: &["%/%", "vs/my-vars"],
                expected_items: &["all"],
                expected_shadowed: &["vs/my-vars"],
            },
            Case {
                name: "exact type broad shadows exact selector of same type",
                input: &["ss/%", "ss/db-creds"],
                expected_items: &["all-by-type:secrets:%"],
                expected_shadowed: &["ss/db-creds"],
            },
            Case {
                name: "exact type broad does not shadow selector of different type",
                input: &["ss/%", "vs/my-vars"],
                expected_items: &["all-by-type:secrets:%", "exact:variables:my-vars"],
                expected_shadowed: &[],
            },
            Case {
                name: "any-type exact ref is preserved",
                input: &["%/db-creds"],
                expected_items: &["any-type-exact:%/db-creds"],
                expected_shadowed: &[],
            },
            Case {
                name: "any-type name pattern is preserved",
                input: &["%/db-%"],
                expected_items: &["any-type-name:%/db-%"],
                expected_shadowed: &[],
            },
            Case {
                name: "any-type exact ref is not shadowed by a single-type broad selector",
                input: &["ss/%", "%/db-creds"],
                expected_items: &["all-by-type:secrets:%", "any-type-exact:%/db-creds"],
                expected_shadowed: &[],
            },
        ];

        for case in cases {
            let syntax = interpret(case.input).await;

            assert_eq!(
                describe_items(&syntax),
                case.expected_items,
                "items mismatch for case: {}",
                case.name,
            );
            assert_eq!(
                describe_shadowed(&syntax),
                case.expected_shadowed,
                "shadowed mismatch for case: {}",
                case.name,
            );
        }
    }

    #[tokio::test]
    async fn test_interpret_same_type_matrix() {
        struct Case {
            name: &'static str,
            input: &'static [&'static str],
            expected_items: &'static [&'static str],
            expected_shadowed: &'static [&'static str],
        }

        let cases = [
            Case {
                name: "same-type name pattern",
                input: &["vs", "app-%"],
                expected_items: &["name-pattern:variables:app-%"],
                expected_shadowed: &[],
            },
            Case {
                name: "same-type all remains case insensitive",
                input: &["vs", "ALL", "my-vars"],
                expected_items: &["all-by-type:variables:ALL"],
                expected_shadowed: &["my-vars"],
            },
        ];

        for case in cases {
            let syntax = interpret(case.input).await;

            assert_eq!(
                describe_items(&syntax),
                case.expected_items,
                "items mismatch for case: {}",
                case.name,
            );
            assert_eq!(
                describe_shadowed(&syntax),
                case.expected_shadowed,
                "shadowed mismatch for case: {}",
                case.name,
            );
        }
    }

    #[test]
    fn test_by_id_syntax_bare_uuid_produces_exact_any_type_item() {
        let uuid = uuid::Uuid::new_v4();
        let resolved = ResolvedResourceSelector {
            input: uuid.to_string(),
            resource_ref: ResourceRef::ById(kamu_resources::ResourceID::new(uuid)),
        };

        let syntax = ResourceSelectionSyntaxServiceImpl::by_id_syntax(resolved);

        assert_eq!(
            describe_items(&syntax),
            vec![format!("exact-any-type:{uuid}")]
        );
        assert!(describe_shadowed(&syntax).is_empty());
    }

    #[tokio::test]
    async fn test_interpret_all_variant_matrix() {
        struct Case {
            name: &'static str,
            input: &'static [&'static str],
            expected_items: &'static [&'static str],
            expected_shadowed: &'static [&'static str],
        }

        let cases = [
            Case {
                name: "global all keeps shadowed ref selector",
                input: &["all", "vs/app-%"],
                expected_items: &["all"],
                expected_shadowed: &["vs/app-%"],
            },
            Case {
                name: "global all keeps shadowed plain selector",
                input: &["all", "my-vars"],
                expected_items: &["all"],
                expected_shadowed: &["my-vars"],
            },
        ];

        for case in cases {
            let syntax = interpret(case.input).await;

            assert_eq!(
                describe_items(&syntax),
                case.expected_items,
                "items mismatch for case: {}",
                case.name,
            );
            assert_eq!(
                describe_shadowed(&syntax),
                case.expected_shadowed,
                "shadowed mismatch for case: {}",
                case.name,
            );
        }
    }

    #[tokio::test]
    async fn test_interpret_all_rejects_unsupported_shadowed_type() {
        assert_matches!(try_interpret(&["all", "unknown/foo"]).await, Err(_));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[tokio::test]
    async fn test_interpret_rejects_type_wildcards() {
        // `%` alone means "all types"; every other wildcard spelling in the type
        // position is a usage error rather than a pattern over type names.
        for input in [
            &["s%/db-creds"][..],
            &["%sets/db-creds"][..],
            &["%TS/db-creds"][..],
            &["s%/%"][..],
            &["s%", "db-creds"][..],
            &["%sets", "all"][..],
            &["all", "unknown%/foo"][..],
        ] {
            let result = try_interpret(input).await;

            assert_matches!(
                result,
                Err(ref e) if e.to_string().contains("Unsupported get target"),
                "expected a usage error for {input:?}",
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
