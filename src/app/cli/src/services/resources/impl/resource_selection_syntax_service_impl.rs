// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::CLIError;
use crate::resources::{
    ResolvedResourceSelectorByKind,
    ResourceKindLookupErrorOptions,
    ResourceKindLookupService,
    ResourceSelectionSyntax,
    ResourceSelectionSyntaxService,
    ResourceSelectorResolutionService,
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

        let selectors = match parsed {
            ParsedSyntax::SameKind {
                kind_str,
                selector_inputs,
            } => {
                let kind_descriptor = self
                    .resource_kind_lookup_service
                    .resolve_kind_descriptor(
                        explicit_context_name,
                        kind_str,
                        ResourceKindLookupErrorOptions::new("Unsupported get target"),
                    )
                    .await?;

                let mut selectors = Vec::with_capacity(selector_inputs.len());
                for selector_input in selector_inputs {
                    let resolved = self
                        .resource_selector_resolution_service
                        .resolve_single_selector(selector_input)
                        .await?;
                    selectors.push(ResolvedResourceSelectorByKind {
                        kind_descriptor: kind_descriptor.clone(),
                        selector_input: resolved.input,
                        resource_ref: resolved.resource_ref,
                    });
                }
                selectors
            }

            ParsedSyntax::RefForm { pairs } => {
                let mut selectors = Vec::with_capacity(pairs.len());
                for (kind_str, selector_input) in pairs {
                    let kind_descriptor = self
                        .resource_kind_lookup_service
                        .resolve_kind_descriptor(
                            explicit_context_name,
                            kind_str,
                            ResourceKindLookupErrorOptions::new("Unsupported get target"),
                        )
                        .await?;
                    let resolved = self
                        .resource_selector_resolution_service
                        .resolve_single_selector(selector_input)
                        .await?;
                    selectors.push(ResolvedResourceSelectorByKind {
                        kind_descriptor,
                        selector_input: resolved.input,
                        resource_ref: resolved.resource_ref,
                    });
                }
                selectors
            }
        };

        Ok(ResourceSelectionSyntax { selectors })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum ParsedSyntax<'a> {
    /// `kind sel1 sel2 ...` — kind is a plain word, selectors have no `/`
    SameKind {
        kind_str: &'a str,
        selector_inputs: Vec<&'a str>,
    },
    /// `kind/sel1 kind/sel2 ...` — every arg contains exactly one `/`
    RefForm { pairs: Vec<(&'a str, &'a str)> },
}

impl ResourceSelectionSyntaxServiceImpl {
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
            return Err(CLIError::usage_error(
                "Cannot mix positional `kind name` and slash `kind/name` syntax in the same \
                 command",
            ));
        }

        if has_slash {
            // Ref form: every arg must be `kind/selector`
            let mut pairs = Vec::with_capacity(args.len());
            for arg in args {
                let parts: Vec<&str> = arg.splitn(2, '/').collect();
                if parts.len() == 2
                    && !parts[0].is_empty()
                    && !parts[1].is_empty()
                    && !parts[1].contains('/')
                {
                    pairs.push((parts[0], parts[1]));
                } else {
                    return Err(CLIError::usage_error(format!(
                        "Invalid resource reference `{arg}`. Expected `kind/name`"
                    )));
                }
            }
            Ok(ParsedSyntax::RefForm { pairs })
        } else {
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
