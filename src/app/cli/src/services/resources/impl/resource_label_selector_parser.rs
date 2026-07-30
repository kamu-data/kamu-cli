// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use kamu_resources::ResourceLabelFilterInput;

use super::resource_label_selector_scanner::{ResourceLabelSelectorScanner, Spanned, Token};
use crate::CLIError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A `key=value` equality predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Predicate {
    key: String,
    value: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Parses `--label`/`-l` arguments into the wire-level
/// [`ResourceLabelFilterInput`] consumed by the facade.
///
/// The scanner has already resolved escapes and dropped whitespace, so the
/// grammar mentions neither:
///
/// ```ebnf
/// selector    = [ conjunction ] , eof ;
/// conjunction = predicate , { separator , predicate } ;
/// predicate   = word , equals , word ;
/// ```
///
/// `=` and `,` are reserved delimiters everywhere, including inside values: a
/// second `=` in a predicate is a syntax error, not part of the value. Use
/// `\=` to mean a literal one. Only the conjunctive fragment is expressible on
/// purpose — the resolved filter is a full boolean tree (`Eq`/`And`/`Not`/`Or`)
/// but evaluation is AND-only, enforced once by `flatten_conjunction` in the
/// domain crate. The reserved sigils are rejected *now* so adding `$not`/`$or`
/// later is a grammar extension rather than a breaking reinterpretation of
/// input that parses today.
///
/// Keys and values are passed through verbatim once unescaped — alias
/// resolution and schema validation belong to the facade, never here.
pub struct ResourceLabelSelectorParser;

impl ResourceLabelSelectorParser {
    /// Parses every `--label` occurrence into a single conjunctive filter.
    ///
    /// Repeated flags accumulate, so `-l a=1 -l b=2` is equivalent to
    /// `-l a=1,b=2`. Returns `None` when no selector was given at all, which
    /// the facade distinguishes from an empty filter.
    pub fn parse(label_selectors: &[String]) -> Result<Option<ResourceLabelFilterInput>, CLIError> {
        if label_selectors.is_empty() {
            return Ok(None);
        }

        let mut entries = BTreeMap::new();

        for label_selector in label_selectors {
            for predicate in Self::parse_one(label_selector)? {
                // `BTreeMap::insert` would silently keep the last value, hiding
                // a contradictory selector such as `-l env=prod,env=staging`.
                if entries
                    .insert(
                        predicate.key.clone(),
                        serde_json::Value::String(predicate.value),
                    )
                    .is_some()
                {
                    return Err(CLIError::usage_error(format!(
                        "Duplicate label selector key `{}`",
                        predicate.key
                    )));
                }
            }
        }

        Ok(Some(ResourceLabelFilterInput { entries }))
    }

    fn parse_one(input: &str) -> Result<Vec<Predicate>, CLIError> {
        let tokens = ResourceLabelSelectorScanner::scan(input)
            .map_err(|err| Self::error_at(input, err.offset, &err.message))?;

        Self::parse_tokens(&tokens, input)
    }

    /// `selector = [ conjunction ] , eof`
    ///
    /// An argument that scans to no tokens at all yields no predicates rather
    /// than an error, so `-l ''` is a no-op instead of a parse failure.
    fn parse_tokens(tokens: &[Spanned], input: &str) -> Result<Vec<Predicate>, CLIError> {
        if tokens.is_empty() {
            return Ok(Vec::new());
        }

        let mut predicates = Vec::new();
        let mut rest = tokens;

        loop {
            let (predicate, remainder) = Self::parse_predicate(rest, input)?;
            predicates.push(predicate);

            match remainder.split_first() {
                None => return Ok(predicates),
                Some((separator, after)) if separator.token == Token::Separator => {
                    if after.is_empty() {
                        return Err(Self::error_at(
                            input,
                            separator.end,
                            "expected another `key=value` predicate after `,`",
                        ));
                    }
                    rest = after;
                }
                Some((unexpected, _)) => {
                    return Err(Self::error_at(
                        input,
                        unexpected.start,
                        "expected `,` between predicates",
                    ));
                }
            }
        }
    }

    /// `predicate = word , equals , word`
    ///
    /// Both sides are mandatory: label values have a minimum length of one, so
    /// `environment=` could never match anything.
    ///
    /// Returns the predicate together with the tokens following it.
    fn parse_predicate<'t>(
        tokens: &'t [Spanned],
        input: &str,
    ) -> Result<(Predicate, &'t [Spanned]), CLIError> {
        let (key_token, rest) = tokens
            .split_first()
            .expect("callers only pass a non-empty token slice");

        let Token::Word(key) = &key_token.token else {
            return Err(Self::error_at(
                input,
                key_token.start,
                "expected a label key",
            ));
        };

        let Some((_, rest)) = rest.split_first().filter(|(t, _)| t.token == Token::Equals) else {
            return Err(Self::error_at(
                input,
                key_token.end,
                "expected `=` after the label key",
            ));
        };

        let equals_end = tokens[1].end;

        let Some((value_token, rest)) = rest.split_first() else {
            return Err(Self::error_at(
                input,
                equals_end,
                "expected a label value after `=`",
            ));
        };

        let Token::Word(value) = &value_token.token else {
            // Reported ahead of the caller's generic "expected `,`" because
            // naming the escape is what the user actually needs. `=` is a
            // delimiter everywhere, so `token=a=b` does not mean `a=b`.
            let message = if value_token.token == Token::Equals {
                r"unexpected `=` in a label value — escape it as `\=`"
            } else {
                "expected a label value after `=`"
            };

            return Err(Self::error_at(input, value_token.start, message));
        };

        // `=` is a delimiter everywhere, so a further one after a complete
        // value is equally a syntax error rather than value text.
        if let Some(equals) = rest.first().filter(|t| t.token == Token::Equals) {
            return Err(Self::error_at(
                input,
                equals.start,
                r"unexpected `=` in a label value — escape it as `\=`",
            ));
        }

        Ok((
            Predicate {
                key: key.clone(),
                value: value.clone(),
            },
            rest,
        ))
    }

    /// Renders a failure as a caret-annotated usage error, so the offending
    /// column is shown rather than described.
    fn error_at(input: &str, offset: usize, message: &str) -> CLIError {
        let caret = " ".repeat(input[..offset.min(input.len())].chars().count());

        CLIError::usage_error(format!(
            "Invalid label selector:\n  {input}\n  {caret}^\n{message}"
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use pretty_assertions::assert_eq;

    use super::*;

    fn selectors(v: &[&str]) -> Vec<String> {
        v.iter().map(ToString::to_string).collect()
    }

    fn parse_entries(v: &[&str]) -> BTreeMap<String, serde_json::Value> {
        ResourceLabelSelectorParser::parse(&selectors(v))
            .expect("selector should parse")
            .expect("selector should not be empty")
            .entries
    }

    fn string_entries(pairs: &[(&str, &str)]) -> BTreeMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(key, value)| {
                (
                    (*key).to_string(),
                    serde_json::Value::String((*value).to_string()),
                )
            })
            .collect()
    }

    // Accepted language

    #[test]
    fn no_selector_flags_yield_no_filter() {
        assert_matches!(ResourceLabelSelectorParser::parse(&[]), Ok(None));
    }

    #[test]
    fn parses_a_single_predicate() {
        assert_eq!(
            parse_entries(&["environment=production"]),
            string_entries(&[("environment", "production")])
        );
    }

    #[test]
    fn parses_a_conjunction_within_one_flag() {
        assert_eq!(
            parse_entries(&["environment=production,tier=backend"]),
            string_entries(&[("environment", "production"), ("tier", "backend")])
        );
    }

    #[test]
    fn repeated_flags_accumulate() {
        assert_eq!(
            parse_entries(&["environment=production", "tier=backend"]),
            string_entries(&[("environment", "production"), ("tier", "backend")])
        );
    }

    #[test]
    fn whitespace_around_tokens_is_insignificant() {
        assert_eq!(
            parse_entries(&["  environment = production , tier = backend "]),
            string_entries(&[("environment", "production"), ("tier", "backend")])
        );
    }

    #[test]
    fn whitespace_inside_a_term_is_preserved() {
        assert_eq!(
            parse_entries(&["owner=data platform"]),
            string_entries(&[("owner", "data platform")])
        );
    }

    #[test]
    fn rejects_a_missing_value() {
        // Label values have a minimum length of one, so `environment=` could
        // never match anything.
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&["environment="])),
            Err(ref e) if e.to_string().contains("expected a label value after `=`")
        );
    }

    #[test]
    fn rejects_a_missing_value_before_a_separator() {
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&["environment=,tier=backend"])),
            Err(ref e) if e.to_string().contains("expected a label value after `=`")
        );
    }

    #[test]
    fn an_entirely_blank_selector_contributes_no_predicates() {
        let filter = ResourceLabelSelectorParser::parse(&selectors(&["   "]))
            .expect("blank selector should parse")
            .expect("a flag was still given");
        assert!(filter.entries.is_empty());
    }

    #[test]
    fn a_canonical_uri_key_is_accepted() {
        assert_eq!(
            parse_entries(&["https://kamu.dev/schemas/resource/v1alpha1/labels/Environment=prod"]),
            string_entries(&[(
                "https://kamu.dev/schemas/resource/v1alpha1/labels/Environment",
                "prod"
            )])
        );
    }

    // Escapes

    #[test]
    fn escapes_resolve_to_their_literal_character() {
        assert_eq!(
            parse_entries(&[r"key\,with\,commas=value\=with\=equals"]),
            string_entries(&[("key,with,commas", "value=with=equals")])
        );
        assert_eq!(
            parse_entries(&[r"path=C:\\tmp"]),
            string_entries(&[("path", r"C:\tmp")])
        );
    }

    #[test]
    fn an_escaped_separator_does_not_split_predicates() {
        assert_eq!(
            parse_entries(&[r"owner=team\,platform"]),
            string_entries(&[("owner", "team,platform")])
        );
    }

    #[test]
    fn reserved_sigils_are_usable_when_escaped() {
        assert_eq!(
            parse_entries(&[r"cost\$center=eng"]),
            string_entries(&[("cost$center", "eng")])
        );
    }

    #[test]
    fn an_unknown_escape_is_rejected_rather_than_passed_through() {
        // Keeping `\n` literal today would make introducing a real newline
        // escape later a silent behavior change.
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&[r"pattern=a\nb"])),
            Err(ref e) if e.to_string().contains("after a backslash")
        );
    }

    #[test]
    fn a_trailing_backslash_is_rejected() {
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&[r"key=value\"])),
            Err(_)
        );
    }

    // Reserved syntax

    #[test]
    fn combinator_sigils_are_reserved() {
        // `$not`/`$or` are representable in the resolved filter tree and
        // reachable over the API; reserving the sigils keeps the door open to
        // spelling them in the CLI without reinterpreting existing input.
        for reserved in [
            "$or(env=prod,env=staging)",
            "$not(env=prod)",
            "env=$prod",
            "env!=prod",
        ] {
            assert_matches!(
                ResourceLabelSelectorParser::parse(&selectors(&[reserved])),
                Err(_),
                "expected `{reserved}` to be rejected as reserved syntax"
            );
        }
    }

    // Rejected input

    #[test]
    fn rejects_a_second_equals_in_a_value() {
        // `=` is a delimiter everywhere; a value containing one must escape it.
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&["token=a=b"])),
            Err(ref e) if e.to_string().contains("unexpected `=` in a label value")
        );
    }

    #[test]
    fn rejects_a_duplicate_key_within_one_flag() {
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&["env=prod,env=staging"])),
            Err(ref e) if e.to_string().contains("Duplicate")
        );
    }

    #[test]
    fn rejects_a_duplicate_key_across_flags() {
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&["env=prod", "env=prod"])),
            Err(ref e) if e.to_string().contains("Duplicate")
        );
    }

    #[test]
    fn rejects_a_predicate_without_an_equals() {
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&["environment"])),
            Err(ref e) if e.to_string().contains("expected `=` after the label key")
        );
    }

    #[test]
    fn rejects_an_empty_key() {
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&["=production"])),
            Err(ref e) if e.to_string().contains("expected a label key")
        );
    }

    #[test]
    fn rejects_an_empty_predicate_between_separators() {
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&["env=prod,,tier=backend"])),
            Err(ref e) if e.to_string().contains("expected a label key")
        );
    }

    #[test]
    fn rejects_a_trailing_separator() {
        assert_matches!(
            ResourceLabelSelectorParser::parse(&selectors(&["env=prod,"])),
            Err(ref e) if e.to_string().contains("after `,`")
        );
    }

    #[test]
    fn a_syntax_error_points_at_the_offending_column() {
        let error = ResourceLabelSelectorParser::parse(&selectors(&["env=prod,tier"]))
            .expect_err("should be rejected");
        let rendered = error.to_string();

        assert!(
            rendered.contains("env=prod,tier"),
            "error should echo the input: {rendered}"
        );
        assert!(
            rendered.contains('^'),
            "error should carry a caret: {rendered}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
