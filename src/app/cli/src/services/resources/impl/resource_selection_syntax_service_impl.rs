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

use fixedbitset::FixedBitSet;
use kamu_resources::ResourceKindDescriptor;

use super::resource_selection_syntax_parser::{
    ALL_SELECTOR,
    ParsedSyntax,
    ResourceSelectionSyntaxParser,
};
use crate::CLIError;
use crate::resources::{
    ResolvedResourceSelector,
    ResourceExactSelector,
    ResourceKindLookupErrorOptions,
    ResourceKindLookupService,
    ResourceSelectionItem,
    ResourceSelectionSyntax,
    ResourceSelectionSyntaxService,
    ResourceSelectorResolutionService,
    ResourceShadowedSelector,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceSelectionSyntaxService)]
pub struct ResourceSelectionSyntaxServiceImpl {
    resource_kind_lookup_service: Arc<dyn ResourceKindLookupService>,
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

        let supported_kinds = self
            .resource_kind_lookup_service
            .list_supported_kinds(explicit_context_name)
            .await?;

        Self::interpret_parsed_syntax_with(&supported_kinds, parsed, |kind_str, selector_input| {
            self.make_selector_item(&supported_kinds, kind_str, selector_input)
        })
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSelectionSyntaxServiceImpl {
    async fn interpret_parsed_syntax_with<F, Fut>(
        supported_kinds: &[ResourceKindDescriptor],
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
            ParsedSyntax::All { shadowed_inputs } => {
                items.push(ResourceSelectionItem::All);

                // Shadowed selectors are reported separately instead of being kept in
                // `items`, so downstream resolution never wastes backend lookups on
                // selectors whose scope is already covered by `all`.
                for shadowed_input in shadowed_inputs {
                    if let Some(kind_str) = shadowed_input.kind_str {
                        Self::validate_shadowed_kind_target(supported_kinds, kind_str)?;
                    }
                    shadowed_selectors.push(ResourceShadowedSelector {
                        selector_input: shadowed_input.display.to_owned(),
                    });
                }
            }

            // kind sel1 sel2 ..
            ParsedSyntax::SameKind {
                kind_str,
                selector_inputs,
            } => {
                // Same-kind syntax has a single explicit kind and only varies by
                // selector names. The only broad token in this mode is `all`,
                // so coverage checks are O(n) and local to the selector list.

                // Note: parsing ensured `kind_str` is never "all" or "%", that goes to
                // `ParsedSyntax::All`. `kind_str` can still be a pattern.

                if selector_inputs
                    .iter()
                    .any(|selector_input| selector_input.eq_ignore_ascii_case(ALL_SELECTOR))
                {
                    // Keep the original `all` spelling as typed by user in
                    // `selector_input` for diagnostics output.
                    items.push(Self::make_all_by_kind_item(
                        supported_kinds,
                        kind_str,
                        selector_inputs
                            .iter()
                            .find(|selector_input| {
                                selector_input.eq_ignore_ascii_case(ALL_SELECTOR)
                            })
                            .expect("same-kind all selector should exist")
                            .to_string(),
                    )?);

                    // Any concrete selector for the same kind is fully covered
                    // by `<kind>/all`, so record it as shadowed instead of
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
                            make_selector_item(kind_str.to_owned(), selector_input.to_owned())
                                .await?,
                        );
                    }
                }
            }

            // kind1/sel1 kind2/sel2 ..
            ParsedSyntax::RefForm { pairs } => {
                // Broad selector coverage is represented as a bit mask over the
                // fixed supported-kind ordering, so superset/equality checks do
                // not allocate or compare string sets.
                let mut broad_selector_coverages = Vec::new();
                let mut broad_selector_union_coverage =
                    KindCoverageMask::new(supported_kinds.len());
                for (kind_str, selector_input) in &pairs {
                    if let Some(coverage) = Self::ref_form_broad_kind_coverage(
                        supported_kinds,
                        kind_str,
                        selector_input,
                    ) {
                        broad_selector_union_coverage.union_with(&coverage);
                        Self::push_unique_coverage(&mut broad_selector_coverages, coverage);
                    }
                }

                // Avoid emitting duplicate broad selectors that expand to exactly
                // the same set of target kinds (e.g. aliases or equivalent
                // patterns), while still reporting shadowed specifics.
                let mut emitted_broad_selectors = Vec::new();

                for (kind_str, selector_input) in pairs {
                    let selector_display = format!("{kind_str}/{selector_input}");
                    // Compute the kind set this selector can possibly touch.
                    // If kind lookup fails, treat it as a regular selector and
                    // defer error handling to item construction.
                    let Some(current_selector_matched_kinds) =
                        Self::kind_coverage_for_selector_target(supported_kinds, kind_str)
                    else {
                        items.push(
                            make_selector_item(kind_str.to_owned(), selector_input.to_owned())
                                .await?,
                        );
                        continue;
                    };

                    let is_broad_selector = Self::is_ref_form_broad_selector(selector_input);
                    // Broad selector is shadowed when some other broad selector
                    // covers a strict superset of its matched kinds.
                    let is_shadowed_by_broader_selector =
                        broad_selector_coverages.iter().any(|broad_coverage| {
                            broad_coverage != &current_selector_matched_kinds
                                && broad_coverage.is_superset(&current_selector_matched_kinds)
                        });

                    if is_shadowed_by_broader_selector {
                        shadowed_selectors.push(ResourceShadowedSelector {
                            selector_input: selector_display,
                        });
                        continue;
                    }

                    if is_broad_selector {
                        // Emit only once per covered kind-set to keep `items`
                        // minimal and deterministic.
                        let emission_key = current_selector_matched_kinds.clone();
                        if Self::push_unique_coverage(&mut emitted_broad_selectors, emission_key) {
                            items.push(Self::make_ref_form_broad_item(
                                supported_kinds,
                                kind_str,
                                selector_input.to_owned(),
                            )?);
                        }
                    } else if broad_selector_union_coverage
                        .is_superset(&current_selector_matched_kinds)
                    {
                        // Narrow selector in ref-form is shadowed when its
                        // matched kind-set is contained in the union of broad
                        // selector coverages.
                        shadowed_selectors.push(ResourceShadowedSelector {
                            selector_input: selector_display,
                        });
                    } else {
                        items.push(
                            make_selector_item(kind_str.to_owned(), selector_input.to_owned())
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

    fn is_pattern(input: &str) -> bool {
        input.contains('%')
    }

    fn make_all_by_kind_item(
        supported_kinds: &[ResourceKindDescriptor],
        kind_str: &str,
        selector_input: String,
    ) -> Result<ResourceSelectionItem, CLIError> {
        if Self::is_pattern(kind_str) {
            if kind_str == "%" {
                return Ok(ResourceSelectionItem::All);
            }
            return Ok(ResourceSelectionItem::KindPatternAll {
                kind_pattern: kind_str.to_owned(),
                selector_input,
            });
        }

        let kind_descriptor = Self::resolve_kind_descriptor(
            supported_kinds,
            kind_str,
            &ResourceKindLookupErrorOptions::new("Unsupported get target"),
        )?;

        Ok(ResourceSelectionItem::AllByKind {
            kind_descriptor,
            selector_input,
        })
    }

    fn make_ref_form_broad_item(
        supported_kinds: &[ResourceKindDescriptor],
        kind_str: &str,
        selector_input: String,
    ) -> Result<ResourceSelectionItem, CLIError> {
        if selector_input == "%" {
            return Self::make_all_by_kind_item(supported_kinds, kind_str, selector_input);
        }

        Self::make_all_by_kind_item(supported_kinds, kind_str, selector_input)
    }

    fn validate_shadowed_kind_target(
        supported_kinds: &[ResourceKindDescriptor],
        kind_str: &str,
    ) -> Result<(), CLIError> {
        if Self::is_pattern(kind_str) {
            if supported_kinds
                .iter()
                .any(|descriptor| descriptor.matches_selector_pattern(kind_str))
            {
                return Ok(());
            }

            return Err(CLIError::usage_error(format!(
                "Unsupported get target '{kind_str}'. Supported targets: {}",
                Self::supported_targets(supported_kinds, &[]).join(", ")
            )));
        }

        Self::resolve_kind_descriptor(
            supported_kinds,
            kind_str,
            &ResourceKindLookupErrorOptions::new("Unsupported get target"),
        )?;

        Ok(())
    }

    async fn make_selector_item(
        &self,
        supported_kinds: &[ResourceKindDescriptor],
        kind_str: String,
        selector_input: String,
    ) -> Result<ResourceSelectionItem, CLIError> {
        Self::make_selector_item_with(
            supported_kinds,
            kind_str,
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
        supported_kinds: &[ResourceKindDescriptor],
        kind_str: String,
        selector_input: String,
        resolve_selector: F,
    ) -> Result<ResourceSelectionItem, CLIError>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<ResolvedResourceSelector, CLIError>>,
    {
        match (
            Self::is_pattern(kind_str.as_str()),
            Self::is_pattern(selector_input.as_str()),
        ) {
            (false, false) => {
                let kind_descriptor = Self::resolve_kind_descriptor(
                    supported_kinds,
                    kind_str.as_str(),
                    &ResourceKindLookupErrorOptions::new("Unsupported get target"),
                )?;
                let resolved = resolve_selector(selector_input).await?;

                Ok(ResourceSelectionItem::Exact(ResourceExactSelector {
                    kind_descriptor,
                    selector_input: resolved.input,
                    resource_ref: resolved.resource_ref,
                }))
            }
            (false, true) => {
                let kind_descriptor = Self::resolve_kind_descriptor(
                    supported_kinds,
                    kind_str.as_str(),
                    &ResourceKindLookupErrorOptions::new("Unsupported get target"),
                )?;

                Ok(ResourceSelectionItem::NamePattern {
                    kind_descriptor,
                    selector_input: selector_input.clone(),
                    name_pattern: selector_input,
                })
            }
            (true, false) => {
                let resolved = resolve_selector(selector_input.clone()).await?;

                Ok(ResourceSelectionItem::KindPatternExactName {
                    kind_pattern: kind_str.clone(),
                    selector_input: format!("{kind_str}/{}", resolved.input),
                    resource_ref: resolved.resource_ref,
                })
            }
            (true, true) => Ok(ResourceSelectionItem::KindPatternNamePattern {
                kind_pattern: kind_str.clone(),
                selector_input: format!("{kind_str}/{selector_input}"),
                name_pattern: selector_input,
            }),
        }
    }

    fn resolve_kind_descriptor(
        supported_kinds: &[ResourceKindDescriptor],
        target: &str,
        error_options: &ResourceKindLookupErrorOptions,
    ) -> Result<ResourceKindDescriptor, CLIError> {
        supported_kinds
            .iter()
            .find(|descriptor| descriptor.matches_selector(target))
            .cloned()
            .ok_or_else(|| {
                CLIError::usage_error(format!(
                    "{} '{target}'. Supported targets: {}",
                    error_options.unsupported_prefix,
                    Self::supported_targets(supported_kinds, &error_options.additional_targets)
                        .join(", ")
                ))
            })
    }

    fn supported_targets(
        supported_kinds: &[ResourceKindDescriptor],
        additional_targets: &[String],
    ) -> Vec<String> {
        let mut targets = additional_targets.to_vec();

        for descriptor in supported_kinds {
            targets.push(descriptor.name.clone());
            targets.extend(descriptor.short_names.iter().cloned());
        }

        targets.sort();
        targets.dedup();
        targets
    }

    fn is_ref_form_broad_selector(selector_input: &str) -> bool {
        selector_input.eq_ignore_ascii_case(ALL_SELECTOR) || selector_input == "%"
    }

    fn ref_form_broad_kind_coverage(
        supported_kinds: &[ResourceKindDescriptor],
        kind_str: &str,
        selector_input: &str,
    ) -> Option<KindCoverageMask> {
        Self::is_ref_form_broad_selector(selector_input)
            .then(|| Self::kind_coverage_for_selector_target(supported_kinds, kind_str))
            .flatten()
    }

    fn push_unique_coverage(
        coverages: &mut Vec<KindCoverageMask>,
        coverage: KindCoverageMask,
    ) -> bool {
        if coverages.iter().any(|existing| existing == &coverage) {
            false
        } else {
            coverages.push(coverage);
            true
        }
    }

    fn kind_coverage_for_selector_target(
        supported_kinds: &[ResourceKindDescriptor],
        kind_str: &str,
    ) -> Option<KindCoverageMask> {
        if Self::is_pattern(kind_str) {
            let mut matched_kinds = KindCoverageMask::new(supported_kinds.len());
            for (index, descriptor) in supported_kinds.iter().enumerate() {
                if descriptor.matches_selector_pattern(kind_str) {
                    matched_kinds.insert(index);
                }
            }

            return Some(matched_kinds);
        }

        supported_kinds
            .iter()
            .enumerate()
            .find(|(_, descriptor)| descriptor.matches_selector(kind_str))
            .map(|(index, _)| {
                let mut matched_kinds = KindCoverageMask::new(supported_kinds.len());
                matched_kinds.insert(index);
                matched_kinds
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
struct KindCoverageMask {
    bits: FixedBitSet,
}

impl KindCoverageMask {
    fn new(num_kinds: usize) -> Self {
        Self {
            bits: FixedBitSet::with_capacity(num_kinds),
        }
    }

    fn insert(&mut self, index: usize) {
        self.bits.insert(index);
    }

    fn is_superset(&self, other: &Self) -> bool {
        self.bits.is_superset(&other.bits)
    }

    fn union_with(&mut self, other: &Self) {
        self.bits.union_with(&other.bits);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use kamu_resources_facade::ResourceRef;

    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(ToString::to_string).collect()
    }

    fn supported_kinds() -> Vec<ResourceKindDescriptor> {
        vec![
            ResourceKindDescriptor {
                name: "variables".to_owned(),
                short_names: vec!["vs".to_owned()],
                schema: "dev.kamu/variable/v1".to_owned(),
                list_columns: Vec::new(),
            },
            ResourceKindDescriptor {
                name: "secrets".to_owned(),
                short_names: vec!["ss".to_owned()],
                schema: "dev.kamu/secret/v1".to_owned(),
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

    async fn interpret(selector_args: &[&str]) -> ResourceSelectionSyntax {
        let selector_args = args(selector_args);
        let parsed = ResourceSelectionSyntaxParser::parse(&selector_args).unwrap();
        let supported_kinds = supported_kinds();

        ResourceSelectionSyntaxServiceImpl::interpret_parsed_syntax_with(
            &supported_kinds,
            parsed,
            |kind_str, selector_input| {
                let supported_kinds = supported_kinds.clone();
                async move {
                    ResourceSelectionSyntaxServiceImpl::make_selector_item_with(
                        &supported_kinds,
                        kind_str,
                        selector_input,
                        |selector_input| async move { Ok(resolved_selector(&selector_input)) },
                    )
                    .await
                }
            },
        )
        .await
        .unwrap()
    }

    fn describe_item(item: &ResourceSelectionItem) -> String {
        match item {
            ResourceSelectionItem::All => "all".to_owned(),
            ResourceSelectionItem::AllByKind {
                kind_descriptor,
                selector_input,
            } => format!("all-by-kind:{}:{selector_input}", kind_descriptor.name),
            ResourceSelectionItem::KindPatternAll {
                kind_pattern,
                selector_input,
            } => format!("kind-pattern-all:{kind_pattern}:{selector_input}"),
            ResourceSelectionItem::Exact(ResourceExactSelector {
                kind_descriptor,
                selector_input,
                ..
            }) => format!("exact:{}:{selector_input}", kind_descriptor.name),
            ResourceSelectionItem::NamePattern {
                kind_descriptor,
                selector_input,
                ..
            } => format!("name-pattern:{}:{selector_input}", kind_descriptor.name),
            ResourceSelectionItem::KindPatternExactName {
                kind_pattern,
                selector_input,
                ..
            } => format!("kind-pattern-exact:{kind_pattern}:{selector_input}"),
            ResourceSelectionItem::KindPatternNamePattern {
                kind_pattern,
                selector_input,
                ..
            } => format!("kind-pattern-name:{kind_pattern}:{selector_input}"),
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
                name: "global broad shadows narrower broad",
                input: &["%/%", "s%/%"],
                expected_items: &["all"],
                expected_shadowed: &["s%/%"],
            },
            Case {
                name: "global broad shadows exact kind broad",
                input: &["%/all", "ss/%"],
                expected_items: &["all"],
                expected_shadowed: &["ss/%"],
            },
            Case {
                name: "equal broad coverages emit once without shadowing",
                input: &["ss/%", "secrets/all"],
                expected_items: &["all-by-kind:secrets:%"],
                expected_shadowed: &[],
            },
            Case {
                name: "duplicate alias broad coverage emits once",
                input: &["secrets/%", "ss/%"],
                expected_items: &["all-by-kind:secrets:%"],
                expected_shadowed: &[],
            },
            Case {
                name: "global wildcard normalizes to all",
                input: &["%/%"],
                expected_items: &["all"],
                expected_shadowed: &[],
            },
            Case {
                name: "kind pattern wildcard emits kind-pattern-all",
                input: &["s%/%"],
                expected_items: &["kind-pattern-all:s%:%"],
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
    async fn test_interpret_ref_form_narrow_selector_shadow_matrix() {
        struct Case {
            name: &'static str,
            input: &'static [&'static str],
            expected_items: &'static [&'static str],
            expected_shadowed: &'static [&'static str],
        }

        let cases = [
            Case {
                name: "narrow exact is shadowed by matching broad pattern",
                input: &["s%/%", "ss/db-creds"],
                expected_items: &["kind-pattern-all:s%:%"],
                expected_shadowed: &["ss/db-creds"],
            },
            Case {
                name: "narrow exact is not shadowed by non superset broad pattern",
                input: &["s%/%", "vs/my-vars"],
                expected_items: &["kind-pattern-all:s%:%", "exact:variables:my-vars"],
                expected_shadowed: &[],
            },
            Case {
                name: "global broad shadows exact selector",
                input: &["%/%", "vs/my-vars"],
                expected_items: &["all"],
                expected_shadowed: &["vs/my-vars"],
            },
            Case {
                name: "exact kind broad shadows exact selector of same kind",
                input: &["ss/%", "ss/db-creds"],
                expected_items: &["all-by-kind:secrets:%"],
                expected_shadowed: &["ss/db-creds"],
            },
            Case {
                name: "exact kind broad does not shadow selector of different kind",
                input: &["ss/%", "vs/my-vars"],
                expected_items: &["all-by-kind:secrets:%", "exact:variables:my-vars"],
                expected_shadowed: &[],
            },
            Case {
                name: "kind pattern exact name is preserved",
                input: &["s%/db-creds"],
                expected_items: &["kind-pattern-exact:s%:s%/db-creds"],
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
    async fn test_interpret_same_kind_matrix() {
        struct Case {
            name: &'static str,
            input: &'static [&'static str],
            expected_items: &'static [&'static str],
            expected_shadowed: &'static [&'static str],
        }

        let cases = [
            Case {
                name: "same-kind name pattern",
                input: &["vs", "app-%"],
                expected_items: &["name-pattern:variables:app-%"],
                expected_shadowed: &[],
            },
            Case {
                name: "same-kind all remains case insensitive",
                input: &["vs", "ALL", "my-vars"],
                expected_items: &["all-by-kind:variables:ALL"],
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
    async fn test_interpret_all_rejects_unsupported_shadowed_kind_pattern() {
        let selector_args = args(&["all", "unknown%/foo"]);
        let parsed = ResourceSelectionSyntaxParser::parse(&selector_args).unwrap();
        let supported_kinds = supported_kinds();

        let result = ResourceSelectionSyntaxServiceImpl::interpret_parsed_syntax_with(
            &supported_kinds,
            parsed,
            |kind_str, selector_input| {
                let supported_kinds = supported_kinds.clone();
                async move {
                    ResourceSelectionSyntaxServiceImpl::make_selector_item_with(
                        &supported_kinds,
                        kind_str,
                        selector_input,
                        |selector_input| async move { Ok(resolved_selector(&selector_input)) },
                    )
                    .await
                }
            },
        )
        .await;

        assert!(result.is_err());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
