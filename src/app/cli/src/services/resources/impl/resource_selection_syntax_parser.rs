// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) const ALL_SELECTOR: &str = "all";

/// In the type position this is the sole accepted wildcard, meaning all types;
/// in the name position it is an ordinary `%` pattern matching every name.
pub(super) const ANY_SELECTOR: &str = "%";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(super) enum ParsedSyntax<'a> {
    /// `all` — all resources across supported types; other args are shadowed.
    All {
        shadowed_inputs: Vec<ShadowedParsedInput<'a>>,
    },
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

#[derive(Debug)]
pub(super) struct ShadowedParsedInput<'a> {
    pub(super) type_str: Option<&'a str>,
    pub(super) display: &'a str,
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
            if args
                .first()
                .is_some_and(|arg| arg.eq_ignore_ascii_case(ALL_SELECTOR))
            {
                let mut shadowed_inputs = Vec::new();
                for arg in args {
                    if arg.eq_ignore_ascii_case(ALL_SELECTOR) {
                        continue;
                    }

                    if arg.contains('/') {
                        let (type_str, _) = Self::parse_ref_arg(arg)?;
                        shadowed_inputs.push(ShadowedParsedInput {
                            type_str: Some(type_str),
                            display: arg,
                        });
                    } else {
                        shadowed_inputs.push(ShadowedParsedInput {
                            type_str: None,
                            display: arg,
                        });
                    }
                }
                return Ok(ParsedSyntax::All { shadowed_inputs });
            }

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
            if args[0].eq_ignore_ascii_case(ALL_SELECTOR) {
                let shadowed_inputs = args[1..]
                    .iter()
                    .map(|arg| ShadowedParsedInput {
                        type_str: None,
                        display: arg.as_str(),
                    })
                    .collect();
                return Ok(ParsedSyntax::All { shadowed_inputs });
            }

            // Same-type form: `type sel1 sel2 ...`
            if args.len() < 2 {
                if Self::is_resource_id(&args[0]) {
                    return Ok(ParsedSyntax::ById {
                        input: args[0].as_str(),
                    });
                }

                return Err(CLIError::usage_error(format!(
                    "Invalid resource reference `{}`. Expected `type/name`",
                    args[0]
                )));
            }
            let type_str = args[0].as_str();
            let selector_inputs = args[1..].iter().map(String::as_str).collect();
            Ok(ParsedSyntax::SameType {
                type_str,
                selector_inputs,
            })
        }
    }

    /// Mirrors the `UUIDv4` check in `resolve_single_selector`.
    fn is_resource_id(arg: &str) -> bool {
        uuid::Uuid::parse_str(arg).is_ok_and(|id| id.get_version() == Some(uuid::Version::Random))
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
                "Invalid resource reference `{arg}`. Expected `type/name`"
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

    #[test]
    fn test_parse_syntax_all() {
        let a = args(&["all"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::All {
                shadowed_inputs,
            }) if shadowed_inputs.is_empty()
        );
    }

    #[test]
    fn test_parse_syntax_all_is_case_insensitive() {
        let a = args(&["ALL"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::All {
                shadowed_inputs,
            }) if shadowed_inputs.is_empty()
        );
    }

    #[test]
    fn test_parse_syntax_all_with_shadowed_plain() {
        let a = args(&["all", "some-name"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::All {
                shadowed_inputs,
            }) if shadowed_inputs.len() == 1
                && shadowed_inputs[0].type_str.is_none()
                && shadowed_inputs[0].display == "some-name"
        );
    }

    #[test]
    fn test_parse_syntax_all_with_shadowed_ref_form() {
        let a = args(&["all", "vs/my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::All {
                shadowed_inputs,
            }) if shadowed_inputs.len() == 1
                && shadowed_inputs[0].type_str == Some("vs")
                && shadowed_inputs[0].display == "vs/my-vars"
        );
    }

    #[test]
    fn test_parse_syntax_all_with_mixed_shadowed() {
        let a = args(&["all", "vs/my-vars", "some-name"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::All { shadowed_inputs })
            if shadowed_inputs.len() == 2
                && shadowed_inputs[0].type_str == Some("vs")
                && shadowed_inputs[0].display == "vs/my-vars"
                && shadowed_inputs[1].type_str.is_none()
                && shadowed_inputs[1].display == "some-name"
        );
    }

    #[test]
    fn test_parse_syntax_all_with_mixed_shadowed_is_case_insensitive() {
        let a = args(&["AlL", "vs/my-vars", "some-name"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::All { shadowed_inputs })
            if shadowed_inputs.len() == 2
                && shadowed_inputs[0].type_str == Some("vs")
                && shadowed_inputs[0].display == "vs/my-vars"
                && shadowed_inputs[1].type_str.is_none()
                && shadowed_inputs[1].display == "some-name"
        );
    }

    #[test]
    fn test_parse_syntax_same_type_all() {
        let a = args(&["vs", "all", "my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::SameType {
                type_str: "vs",
                selector_inputs,
            }) if selector_inputs == vec!["all", "my-vars"]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_all_by_type_single() {
        let a = args(&["vs/all"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "all")]
        );
    }

    #[test]
    fn test_parse_syntax_slash_form_all_by_type_with_shadowed() {
        let a = args(&["vs/all", "vs/my-vars"]);
        assert_matches!(
            ResourceSelectionSyntaxParser::parse(&a),
            Ok(ParsedSyntax::RefForm { pairs }) if pairs == vec![("vs", "all"), ("vs", "my-vars")]
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
    fn test_parse_syntax_single_no_slash_is_error() {
        let a = args(&["vs"]);
        assert_matches!(ResourceSelectionSyntaxParser::parse(&a), Err(_));
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
    fn test_parse_syntax_bare_non_v4_uuid_is_error() {
        // A nil (all-zero) UUID is syntactically valid but not version 4, so it
        // must not be treated as a resolvable resource ID.
        let a = args(&["00000000-0000-0000-0000-000000000000"]);
        assert_matches!(ResourceSelectionSyntaxParser::parse(&a), Err(_));
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

    #[test]
    fn test_parse_syntax_non_leading_all_in_mixed_syntax_is_error() {
        let a = args(&["vs", "all", "ss/db-creds"]);
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
