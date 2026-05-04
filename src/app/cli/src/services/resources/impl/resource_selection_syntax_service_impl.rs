// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_resources::ResourceKindDescriptor;

use crate::CLIError;
use crate::resources::{
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
        let parsed = Self::parse_syntax(args)?;

        let supported_kinds = self
            .resource_kind_lookup_service
            .list_supported_kinds(explicit_context_name)
            .await?;

        let mut items = Vec::new();
        let mut shadowed_selectors = Vec::new();

        match parsed {
            ParsedSyntax::All { shadowed_inputs } => {
                items.push(ResourceSelectionItem::All);

                for shadowed_input in shadowed_inputs {
                    if let Some(kind_str) = shadowed_input.kind_str {
                        Self::resolve_kind_descriptor(
                            &supported_kinds,
                            kind_str,
                            &ResourceKindLookupErrorOptions::new("Unsupported get target"),
                        )?;
                    }
                    shadowed_selectors.push(ResourceShadowedSelector {
                        selector_input: shadowed_input.display.to_owned(),
                    });
                }
            }

            ParsedSyntax::SameKind {
                kind_str,
                selector_inputs,
            } => {
                let kind_descriptor = Self::resolve_kind_descriptor(
                    &supported_kinds,
                    kind_str,
                    &ResourceKindLookupErrorOptions::new("Unsupported get target"),
                )?;

                if selector_inputs.contains(&ALL_SELECTOR) {
                    items.push(ResourceSelectionItem::AllByKind {
                        kind_descriptor,
                        selector_input: ALL_SELECTOR.to_owned(),
                    });

                    for selector_input in selector_inputs {
                        if selector_input != ALL_SELECTOR {
                            shadowed_selectors.push(ResourceShadowedSelector {
                                selector_input: selector_input.to_owned(),
                            });
                        }
                    }
                } else {
                    for selector_input in selector_inputs {
                        let resolved = self
                            .resource_selector_resolution_service
                            .resolve_single_selector(selector_input)
                            .await?;
                        items.push(ResourceSelectionItem::Exact(ResourceExactSelector {
                            kind_descriptor: kind_descriptor.clone(),
                            selector_input: resolved.input,
                            resource_ref: resolved.resource_ref,
                        }));
                    }
                }
            }

            ParsedSyntax::RefForm { pairs } => {
                let all_by_kind: std::collections::BTreeSet<&str> = pairs
                    .iter()
                    .filter_map(|(kind_str, selector_input)| {
                        (*selector_input == ALL_SELECTOR).then_some(*kind_str)
                    })
                    .collect();

                let mut emitted_all_by_kind = std::collections::BTreeSet::new();

                for (kind_str, selector_input) in pairs {
                    let kind_descriptor = Self::resolve_kind_descriptor(
                        &supported_kinds,
                        kind_str,
                        &ResourceKindLookupErrorOptions::new("Unsupported get target"),
                    )?;

                    if selector_input == ALL_SELECTOR {
                        if emitted_all_by_kind.insert(kind_str) {
                            items.push(ResourceSelectionItem::AllByKind {
                                kind_descriptor,
                                selector_input: selector_input.to_owned(),
                            });
                        }
                    } else if all_by_kind.contains(kind_str) {
                        shadowed_selectors.push(ResourceShadowedSelector {
                            selector_input: format!("{kind_str}/{selector_input}"),
                        });
                    } else {
                        let resolved = self
                            .resource_selector_resolution_service
                            .resolve_single_selector(selector_input)
                            .await?;
                        items.push(ResourceSelectionItem::Exact(ResourceExactSelector {
                            kind_descriptor,
                            selector_input: resolved.input,
                            resource_ref: resolved.resource_ref,
                        }));
                    }
                }
            }
        }

        Ok(ResourceSelectionSyntax {
            items,
            shadowed_selectors,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ALL_SELECTOR: &str = "all";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum ParsedSyntax<'a> {
    /// `all` — all resources across supported kinds; other args are shadowed.
    All {
        shadowed_inputs: Vec<ShadowedParsedInput<'a>>,
    },
    /// `kind sel1 sel2 ...` — kind is a plain word, selectors have no `/`
    SameKind {
        kind_str: &'a str,
        selector_inputs: Vec<&'a str>,
    },
    /// `kind/sel1 kind/sel2 ...` — every arg contains exactly one `/`
    RefForm { pairs: Vec<(&'a str, &'a str)> },
}

#[derive(Debug)]
struct ShadowedParsedInput<'a> {
    kind_str: Option<&'a str>,
    display: &'a str,
}

impl ResourceSelectionSyntaxServiceImpl {
    fn resolve_kind_descriptor(
        supported_kinds: &[ResourceKindDescriptor],
        target: &str,
        error_options: &ResourceKindLookupErrorOptions,
    ) -> Result<ResourceKindDescriptor, CLIError> {
        supported_kinds
            .iter()
            .find(|descriptor| {
                descriptor.name.eq_ignore_ascii_case(target)
                    || descriptor
                        .short_names
                        .iter()
                        .any(|short_name| short_name.eq_ignore_ascii_case(target))
            })
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

    /// Parses raw CLI `args` into a [`ParsedSyntax`] variant.
    ///
    /// Accepted forms:
    /// - Same-kind: first arg has no `/`, remaining args have no `/`, at least
    ///   two args total.
    /// - Ref form: every arg contains exactly one `/` with non-empty parts on
    ///   both sides.
    /// - Mixed forms are rejected.
    fn parse_syntax(args: &[String]) -> Result<ParsedSyntax<'_>, CLIError> {
        if args.is_empty() {
            return Err(CLIError::usage_error("Expected `kind name` or `kind/name`"));
        }

        let has_slash = args.iter().any(|a| a.contains('/'));
        let has_plain = args.iter().any(|a| !a.contains('/'));

        if has_slash && has_plain {
            if args.iter().any(|arg| arg == ALL_SELECTOR) {
                let mut shadowed_inputs = Vec::new();
                for arg in args {
                    if arg == ALL_SELECTOR {
                        continue;
                    }

                    if arg.contains('/') {
                        let (kind_str, _) = Self::parse_ref_arg(arg)?;
                        shadowed_inputs.push(ShadowedParsedInput {
                            kind_str: Some(kind_str),
                            display: arg,
                        });
                    } else {
                        shadowed_inputs.push(ShadowedParsedInput {
                            kind_str: None,
                            display: arg,
                        });
                    }
                }
                return Ok(ParsedSyntax::All { shadowed_inputs });
            }

            return Err(CLIError::usage_error(
                "Cannot mix positional `kind name` and slash `kind/name` syntax in the same \
                 command",
            ));
        }

        if has_slash {
            // Ref form: every arg must be `kind/selector`
            let mut pairs = Vec::with_capacity(args.len());
            for arg in args {
                pairs.push(Self::parse_ref_arg(arg)?);
            }
            Ok(ParsedSyntax::RefForm { pairs })
        } else {
            if args[0] == ALL_SELECTOR {
                let shadowed_inputs = args[1..]
                    .iter()
                    .map(|arg| ShadowedParsedInput {
                        kind_str: None,
                        display: arg.as_str(),
                    })
                    .collect();
                return Ok(ParsedSyntax::All { shadowed_inputs });
            }

            // Same-kind form: `kind sel1 sel2 ...`
            if args.len() < 2 {
                return Err(CLIError::usage_error(format!(
                    "Invalid resource reference `{}`. Expected `kind/name`",
                    args[0]
                )));
            }
            let kind_str = args[0].as_str();
            let selector_inputs = args[1..].iter().map(String::as_str).collect();
            Ok(ParsedSyntax::SameKind {
                kind_str,
                selector_inputs,
            })
        }
    }

    fn parse_ref_arg(arg: &str) -> Result<(&str, &str), CLIError> {
        let parts: Vec<&str> = arg.splitn(2, '/').collect();
        if parts.len() == 2
            && !parts[0].is_empty()
            && !parts[1].is_empty()
            && !parts[1].contains('/')
        {
            Ok((parts[0], parts[1]))
        } else {
            Err(CLIError::usage_error(format!(
                "Invalid resource reference `{arg}`. Expected `kind/name`"
            )))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn test_parse_syntax_two_plain() {
        let a = args(&["vs", "my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::SameKind {
                kind_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["my-vars"]
        );
    }

    #[test]
    fn test_parse_syntax_same_kind_multiple() {
        let a = args(&["vs", "vars-a", "vars-b"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::SameKind {
                kind_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["vars-a", "vars-b"]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_single() {
        let a = args(&["vs/my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "my-vars")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_multiple() {
        let a = args(&["vs/vars-a", "ss/db-creds"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "vars-a"), ("ss", "db-creds")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_uuid() {
        let a = args(&["vs/3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69")]
        );
    }

    #[test]
    fn test_parse_syntax_all() {
        let a = args(&["all"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::All {
                shadowed_inputs,
            }) if shadowed_inputs.is_empty()
        );
    }

    #[test]
    fn test_parse_syntax_all_with_shadowed_plain() {
        let a = args(&["all", "some-name"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::All {
                shadowed_inputs,
            }) if shadowed_inputs.len() == 1
                && shadowed_inputs[0].kind_str.is_none()
                && shadowed_inputs[0].display == "some-name"
        );
    }

    #[test]
    fn test_parse_syntax_all_with_shadowed_ref_form() {
        let a = args(&["all", "vs/my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::All {
                shadowed_inputs,
            }) if shadowed_inputs.len() == 1
                && shadowed_inputs[0].kind_str == Some("vs")
                && shadowed_inputs[0].display == "vs/my-vars"
        );
    }

    #[test]
    fn test_parse_syntax_all_with_mixed_shadowed() {
        let a = args(&["all", "vs/my-vars", "some-name"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::All { shadowed_inputs })
            if shadowed_inputs.len() == 2
                && shadowed_inputs[0].kind_str == Some("vs")
                && shadowed_inputs[0].display == "vs/my-vars"
                && shadowed_inputs[1].kind_str.is_none()
                && shadowed_inputs[1].display == "some-name"
        );
    }

    #[test]
    fn test_parse_syntax_same_kind_all() {
        let a = args(&["vs", "all", "my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::SameKind {
                kind_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["all", "my-vars"]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_all_by_kind_single() {
        let a = args(&["vs/all"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "all")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_all_by_kind_with_shadowed() {
        let a = args(&["vs/all", "vs/my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "all"), ("vs", "my-vars")]
        );
    }

    #[test]
    fn test_parse_syntax_single_no_slash_is_error() {
        let a = args(&["vs"]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::parse_syntax(&a), Err(_));
    }

    #[test]
    fn test_parse_syntax_slash_missing_name_is_error() {
        let a = args(&["vs/"]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::parse_syntax(&a), Err(_));
    }

    #[test]
    fn test_parse_syntax_slash_form_malformed_second_arg_is_error() {
        // First arg is valid but second is missing the name part
        let a = args(&["vs/vars-a", "ss/"]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::parse_syntax(&a), Err(_));
    }

    #[test]
    fn test_parse_syntax_slash_form_extra_slash_is_error() {
        // `vs/foo/bar` has two slashes — the selector part itself contains `/`
        let a = args(&["vs/foo/bar"]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::parse_syntax(&a), Err(_));
    }

    #[test]
    fn test_parse_syntax_slash_missing_kind_is_error() {
        let a = args(&["/my-vars"]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::parse_syntax(&a), Err(_));
    }

    #[test]
    fn test_parse_syntax_mixed_syntax_is_error() {
        // "vs my-vars ss/db-creds" mixes plain and slash forms
        let a = args(&["vs", "my-vars", "ss/db-creds"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::parse_syntax(&a),
            Err(ref e) if e.to_string().contains("mix")
        );
    }

    #[test]
    fn test_parse_syntax_empty_is_error() {
        let a = args(&[]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::parse_syntax(&a), Err(_));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
