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
        let (kind_str, selector_input) = Self::split_args(args)?;

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

        Ok(ResourceSelectionSyntax {
            selectors: vec![ResolvedResourceSelectorByKind {
                kind_descriptor,
                selector_input: resolved.input,
                resource_ref: resolved.resource_ref,
            }],
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceSelectionSyntaxServiceImpl {
    /// Splits raw CLI `args` into `(kind, selector)` for Phase 2.
    ///
    /// Accepted forms:
    /// - Two args with no `/` in either → `(args[0], args[1])`
    /// - One arg with exactly one `/` → `(left_of_slash, right_of_slash)`
    fn split_args(args: &[String]) -> Result<(&str, &str), CLIError> {
        match args {
            [single] => {
                let parts: Vec<&str> = single.splitn(2, '/').collect();
                if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                    Ok((parts[0], parts[1]))
                } else {
                    Err(CLIError::usage_error(format!(
                        "Invalid resource reference `{single}`. Expected `kind/name`"
                    )))
                }
            }
            [kind, name] if !kind.contains('/') && !name.contains('/') => Ok((kind, name)),
            _ => {
                let has_slash = args.iter().any(|a| a.contains('/'));
                let has_plain = args.iter().any(|a| !a.contains('/'));
                if has_slash && has_plain {
                    Err(CLIError::usage_error(
                        "Cannot mix positional `kind name` and slash `kind/name` syntax in the \
                         same command",
                    ))
                } else {
                    Err(CLIError::usage_error("Expected `kind name` or `kind/name`"))
                }
            }
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
    fn test_split_args_two_plain() {
        let a = args(&["vs", "my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::split_args(&a),
            Ok(("vs", "my-vars"))
        );
    }

    #[test]
    fn test_split_args_slash_form() {
        let a = args(&["vs/my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::split_args(&a),
            Ok(("vs", "my-vars"))
        );
    }

    #[test]
    fn test_split_args_slash_form_uuid() {
        let a = args(&["vs/3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::split_args(&a),
            Ok(("vs", "3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69"))
        );
    }

    #[test]
    fn test_split_args_single_no_slash_is_error() {
        let a = args(&["vs"]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::split_args(&a), Err(_));
    }

    #[test]
    fn test_split_args_slash_missing_name_is_error() {
        let a = args(&["vs/"]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::split_args(&a), Err(_));
    }

    #[test]
    fn test_split_args_slash_missing_kind_is_error() {
        let a = args(&["/my-vars"]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::split_args(&a), Err(_));
    }

    #[test]
    fn test_split_args_mixed_syntax_is_error() {
        // "vs my-vars ss/db-creds" mixes plain and slash forms
        let a = args(&["vs", "my-vars", "ss/db-creds"]);
        assert_matches!(
            ResourceSelectionSyntaxServiceImpl::split_args(&a),
            Err(ref e) if e.to_string().contains("mix")
        );
    }

    #[test]
    fn test_split_args_three_plain_args_is_error() {
        let a = args(&["vs", "a", "b"]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::split_args(&a), Err(_));
    }

    #[test]
    fn test_split_args_empty_is_error() {
        let a = args(&[]);
        assert_matches!(ResourceSelectionSyntaxServiceImpl::split_args(&a), Err(_));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
