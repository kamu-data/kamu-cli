// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::resource_selection_scanner::{ANY_SELECTOR, ResourceSelectionScanner, SelectorArg};
use super::selector_error::usage_error_at;
use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(super) enum ParsedSyntax<'a> {
    /// A single bare arg that parses as a `UUIDv4` resource ID.
    ById { input: &'a str },
    /// `type sel1 sel2 ...` — type is a plain word, selectors have no `/`
    SameType {
        type_str: &'a str,
        selector_inputs: Vec<&'a str>,
    },
    /// `type/sel1 type/sel2 ...` — every arg contains exactly one `/`
    RefForm { pairs: Vec<(&'a str, &'a str)> },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) struct ResourceSelectionSyntaxParser;

impl ResourceSelectionSyntaxParser {
    /// Parses raw CLI `args` into a [`ParsedSyntax`] variant.
    ///
    /// Accepted forms:
    /// - Same-type: first arg has no `/`, remaining args have no `/`, at least
    ///   two args total.
    /// - Ref form: every arg contains exactly one `/` with non-empty parts on
    ///   both sides.
    /// - Mixed forms are rejected.
    pub(super) fn parse(args: &[String]) -> Result<ParsedSyntax<'_>, CLIError> {
        if args.is_empty() {
            return Err(CLIError::usage_error("Expected `type name` or `type/name`"));
        }

        let has_slash = args.iter().any(|a| a.contains('/'));
        let has_plain = args.iter().any(|a| !a.contains('/'));

        if has_slash && has_plain {
            return Err(CLIError::usage_error(
                "Cannot mix positional `type name` and slash `type/name` syntax in the same \
                 command",
            ));
        }

        if has_slash {
            // Ref form: every arg must be `type/selector`
            let mut pairs = Vec::with_capacity(args.len());
            for arg in args {
                pairs.push(Self::parse_ref_arg(arg)?);
            }
            Ok(ParsedSyntax::RefForm { pairs })
        } else {
            // Same-type form: `type sel1 sel2 ...`
            if args.len() < 2 {
                if Self::is_resource_id(&args[0]) {
                    return Ok(ParsedSyntax::ById {
                        input: args[0].as_str(),
                    });
                }

                // A lone bare type is `type/%`. Keyed off argument shape, so
                // `vs ss` stays a name lookup rather than two bare types.
                return Ok(ParsedSyntax::RefForm {
                    pairs: vec![Self::parse_bare_type_arg(&args[0])?],
                });
            }
            let type_str = args[0].as_str();
            let selector_inputs = args[1..].iter().map(String::as_str).collect();
            Ok(ParsedSyntax::SameType {
                type_str,
                selector_inputs,
            })
        }
    }

    fn is_resource_id(arg: &str) -> bool {
        super::resource_ref_classifier::is_resource_id(arg)
    }

    /// Splits a `type/name` argument. Only reached for args carrying a `/`, so
    /// the scanner always yields a name half.
    fn parse_ref_arg(arg: &str) -> Result<(&str, &str), CLIError> {
        let selector = Self::scan_arg(arg)?;

        let name_half = selector
            .name_half
            .expect("an arg containing `/` always scans with a name half");

        Ok((selector.type_half, name_half))
    }

    /// Expands a lone bare `type` into its `type/%` pair. The type half is
    /// validated later, when types are resolved.
    fn parse_bare_type_arg(arg: &str) -> Result<(&str, &str), CLIError> {
        let selector = Self::scan_arg(arg)?;

        debug_assert!(
            selector.name_half.is_none(),
            "a slash-free arg cannot scan with a name half"
        );

        Ok((selector.type_half, ANY_SELECTOR))
    }

    fn scan_arg(arg: &str) -> Result<SelectorArg<'_>, CLIError> {
        ResourceSelectionScanner::scan_selector_arg(arg)
            .map_err(|err| usage_error_at("resource reference", arg, err.offset, &err.message))
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
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::SameType {
                type_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["my-vars"]
        );
    }

    #[test]
    fn test_parse_syntax_same_type_multiple() {
        let a = args(&["vs", "vars-a", "vars-b"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::SameType {
                type_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["vars-a", "vars-b"]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_single() {
        let a = args(&["vs/my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "my-vars")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_multiple() {
        let a = args(&["vs/vars-a", "ss/db-creds"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "vars-a"), ("ss", "db-creds")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_uuid() {
        let a = args(&["vs/3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "3d8d6d1c-6f7c-4c62-9f4e-7d8295e8fb69")]
        );
    }

    // `all` carries no special meaning: it tokenizes as an ordinary name, and
    // resolves as one.
    #[test]
    fn test_parse_syntax_all_is_an_ordinary_name() {
        let a = args(&["vs", "all"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::SameType {
                type_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["all"]
        );
    }

    // A lone bare type is aliased to its `type/%` form, matching `list`.

    #[test]
    fn test_parse_syntax_bare_any_selector_is_all_resources() {
        let a = args(&["%"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("%", "%")]
        );
    }

    #[test]
    fn test_parse_syntax_bare_canonical_type_is_all_of_that_type() {
        let a = args(&["VariableSet"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("VariableSet", "%")]
        );
    }

    // The alias keys off argument shape, not content: a second arg makes the
    // first a type and the rest names.
    #[test]
    fn test_parse_syntax_two_bare_types_stay_a_name_lookup() {
        let a = args(&["vs", "ss"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::SameType {
                type_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["ss"]
        );
    }

    #[test]
    fn test_parse_syntax_same_type_broad() {
        let a = args(&["vs", "%", "my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::SameType {
                type_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["%", "my-vars"]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_all_by_type_single() {
        let a = args(&["vs/%"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "%")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_all_by_type_with_shadowed() {
        let a = args(&["vs/%", "vs/my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "%"), ("vs", "my-vars")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_any_type_all_wildcard() {
        let a = args(&["%/%"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("%", "%")]
        );
    }

    #[test]
    fn test_parse_syntax_same_type_name_pattern() {
        let a = args(&["vs", "app-%"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::SameType {
                type_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["app-%"]
        );
    }

    // Parsing is purely lexical: a `%`-carrying type is tokenized here and
    // accepted or rejected later, when types are resolved.
    #[test]
    fn test_parse_syntax_same_type_wildcard() {
        let a = args(&["%", "db-creds"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::SameType {
                type_str: "%",
                selector_inputs,
            }) if selector_inputs == vec!["db-creds"]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_name_pattern() {
        let a = args(&["vs/app-%"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "app-%")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_any_type() {
        let a = args(&["%/db-creds"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("%", "db-creds")]
        );
    }

    #[test]
    fn test_parse_syntax_single_no_slash_is_all_of_that_type() {
        let a = args(&["vs"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "%")]
        );
    }

    #[test]
    fn test_parse_syntax_bare_uuid_v4_is_by_id() {
        let uuid = uuid::Uuid::new_v4().to_string();
        let a = args(&[&uuid]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::ById { input }) if input == uuid
        );
    }

    #[test]
    fn test_parse_syntax_bare_non_v4_uuid_is_not_an_id() {
        // A nil (all-zero) UUID is syntactically valid but not version 4, so
        // it must never be treated as a resolvable resource ID: it falls
        // through to the bare-type alias, which type resolution then rejects.
        let a = args(&["00000000-0000-0000-0000-000000000000"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs })
                if pairs == vec![("00000000-0000-0000-0000-000000000000", "%")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_missing_name_is_error() {
        let a = args(&["vs/"]);
        assert_matches!(ResourceSelectionSyntaxParser::parse(&a), Err(_));
    }

    #[test]
    fn test_parse_syntax_slash_form_malformed_second_arg_is_error() {
        // First arg is valid but second is missing the name part
        let a = args(&["vs/vars-a", "ss/"]);
        assert_matches!(ResourceSelectionSyntaxParser::parse(&a), Err(_));
    }

    #[test]
    fn test_parse_syntax_slash_form_extra_slash_is_error() {
        // `vs/foo/bar` has two slashes — the selector part itself contains `/`
        let a = args(&["vs/foo/bar"]);
        assert_matches!(ResourceSelectionSyntaxParser::parse(&a), Err(_));
    }

    #[test]
    fn test_parse_syntax_slash_missing_type_is_error() {
        let a = args(&["/my-vars"]);
        assert_matches!(ResourceSelectionSyntaxParser::parse(&a), Err(_));
    }

    #[test]
    fn test_parse_syntax_mixed_syntax_is_error() {
        // "vs my-vars ss/db-creds" mixes plain and slash forms
        let a = args(&["vs", "my-vars", "ss/db-creds"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Err(ref e) if e.to_string().contains("mix")
        );
    }

    // No arg is exempt from the mixing rule — a leading broad selector does not
    // license a plain arg alongside a slash one.
    #[test]
    fn test_parse_syntax_leading_broad_selector_in_mixed_syntax_is_error() {
        let a = args(&["%", "ss/db-creds"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Err(ref e) if e.to_string().contains("mix")
        );
    }

    #[test]
    fn test_parse_syntax_empty_is_error() {
        let a = args(&[]);
        assert_matches!(ResourceSelectionSyntaxParser::parse(&a), Err(_));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
