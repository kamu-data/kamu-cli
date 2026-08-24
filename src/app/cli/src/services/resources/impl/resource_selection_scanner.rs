// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use winnow::combinator::{alt, eof, repeat, terminated};
use winnow::token::{one_of, take_while};
use winnow::{ModalResult, Parser};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Lexical structure
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// In the type position this is the sole accepted wildcard, meaning all types;
/// in the name position it is an ordinary `%` pattern matching every name.
pub const ANY_SELECTOR: &str = "%";

pub const TYPE_NAME_SEPARATOR: char = '/';

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One lexical token.
///
/// There is no escape production, unlike the label selector scanner: both
/// halves are hostname-charset (`Grammar::match_resource_name`), so a `/` can
/// never occur inside a word and nothing needs escaping to be spelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Word(String),
    Separator,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A token plus its byte span in the original input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spanned {
    pub token: Token,
    pub start: usize,
    pub end: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Failure to tokenize.
#[derive(Debug)]
pub struct ScanError {
    pub offset: usize,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Whether a bare `type` with no `/` is a legal argument.
///
/// The only difference between the `list` grammar and the `get`/`delete` one:
/// `list vs` enumerates the type, while `get vs` is a usage error directing the
/// user to `vs/%`. Broadening `get` would widen the blast radius of `delete`,
/// so the two stay apart deliberately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BareTypePolicy {
    Allow,
    Reject,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A single selector argument, split but not interpreted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectorArg<'a> {
    pub type_half: &'a str,
    /// `None` for a bare `type` carrying no `/`.
    pub name_half: Option<&'a str>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceSelectionScanner;

impl ResourceSelectionScanner {
    pub fn scan(input: &str) -> Result<Vec<Spanned>, ScanError> {
        scanner.parse(input).map_err(|err| ScanError {
            offset: err.offset(),
            message: err.inner().to_string(),
        })
    }

    /// Splits one `type[/name]` argument into its halves.
    ///
    /// Purely lexical: a `%`-carrying type half scans happily here and is
    /// accepted or rejected later, when types are resolved.
    pub fn scan_selector_arg(
        input: &str,
        bare_type_policy: BareTypePolicy,
    ) -> Result<SelectorArg<'_>, ScanError> {
        let tokens = Self::scan(input)?;

        let mut words = Vec::new();
        let mut separators = Vec::new();

        for spanned in &tokens {
            match &spanned.token {
                Token::Word(_) => words.push(spanned),
                Token::Separator => separators.push(spanned),
            }
        }

        // A second `/` is reported at its own offset rather than at the end, so
        // the caret lands on the separator that made the argument ambiguous.
        if let Some(extra) = separators.get(1) {
            return Err(ScanError {
                offset: extra.start,
                message: format!("unexpected second `{TYPE_NAME_SEPARATOR}`"),
            });
        }

        let Some(separator) = separators.first() else {
            let Some(type_half) = words.first() else {
                return Err(ScanError {
                    offset: 0,
                    message: "expected a resource type".to_owned(),
                });
            };

            if bare_type_policy == BareTypePolicy::Reject {
                let type_text = &input[type_half.start..type_half.end];

                return Err(ScanError {
                    offset: type_half.end,
                    message: format!(
                        "expected `{TYPE_NAME_SEPARATOR}` after the resource type — write \
                         `{type_text}{TYPE_NAME_SEPARATOR}{ANY_SELECTOR}` to select every one"
                    ),
                });
            }

            return Ok(SelectorArg {
                type_half: &input[type_half.start..type_half.end],
                name_half: None,
            });
        };

        // Exactly one separator, and words never sit adjacent, so the token run
        // is at most `word / word` — a word before the separator is the type
        // half, one after it is the name half, and each is missing when empty.
        let type_half = words
            .iter()
            .find(|word| word.end <= separator.start)
            .ok_or_else(|| ScanError {
                offset: separator.start,
                message: "expected a resource type before the `/`".to_owned(),
            })?;

        let name_half = words
            .iter()
            .find(|word| word.start >= separator.end)
            .ok_or_else(|| ScanError {
                offset: separator.end,
                message: "expected a resource name after the `/`".to_owned(),
            })?;

        Ok(SelectorArg {
            type_half: &input[type_half.start..type_half.end],
            name_half: Some(&input[name_half.start..name_half.end]),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Productions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn scanner(input: &mut &str) -> ModalResult<Vec<Spanned>> {
    let total_len = input.len();

    terminated(repeat(0.., spanned_token(total_len)), eof).parse_next(input)
}

fn spanned_token(total_len: usize) -> impl FnMut(&mut &str) -> ModalResult<Spanned> {
    move |input: &mut &str| {
        let start = total_len - input.len();
        let token = token.parse_next(input)?;
        let end = total_len - input.len();

        Ok(Spanned { token, start, end })
    }
}

fn token(input: &mut &str) -> ModalResult<Token> {
    alt((
        one_of(TYPE_NAME_SEPARATOR).value(Token::Separator),
        word.map(|text: &str| Token::Word(text.to_owned())),
    ))
    .parse_next(input)
}

/// Any run that is not a separator. Whitespace is not special: the shell has
/// already split the arguments, so a space inside one is part of the word.
fn word<'i>(input: &mut &'i str) -> ModalResult<&'i str> {
    take_while(1.., |c: char| c != TYPE_NAME_SEPARATOR).parse_next(input)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use pretty_assertions::assert_eq;

    use super::*;

    fn scan(input: &str) -> Vec<Token> {
        ResourceSelectionScanner::scan(input)
            .expect("input should scan")
            .into_iter()
            .map(|spanned| spanned.token)
            .collect()
    }

    fn word(text: &str) -> Token {
        Token::Word(text.to_string())
    }

    fn split(input: &str) -> SelectorArg<'_> {
        ResourceSelectionScanner::scan_selector_arg(input, BareTypePolicy::Allow)
            .expect("input should split")
    }

    // Tokenization

    #[test]
    fn splits_a_ref_form_argument_into_two_words() {
        assert_eq!(
            scan("vs/my-vars"),
            vec![word("vs"), Token::Separator, word("my-vars")]
        );
    }

    #[test]
    fn a_bare_type_yields_a_single_word() {
        assert_eq!(scan("vs"), vec![word("vs")]);
    }

    #[test]
    fn wildcards_are_ordinary_word_text() {
        // `%` carries no lexical meaning; it is classified when types resolve.
        assert_eq!(scan("%"), vec![word("%")]);
        assert_eq!(scan("%/%"), vec![word("%"), Token::Separator, word("%")]);
        assert_eq!(
            scan("%set/db-%"),
            vec![word("%set"), Token::Separator, word("db-%")]
        );
    }

    #[test]
    fn records_spans_against_the_original_input() {
        let tokens = ResourceSelectionScanner::scan("vs/my-vars").expect("should scan");

        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 2);
        assert_eq!(tokens[1].token, Token::Separator);
        assert_eq!(tokens[1].start, 2);
        assert_eq!(tokens[2].start, 3);
        assert_eq!(tokens[2].end, 10);
    }

    // Splitting

    #[test]
    fn splits_both_halves() {
        assert_eq!(
            split("vs/my-vars"),
            SelectorArg {
                type_half: "vs",
                name_half: Some("my-vars"),
            }
        );
    }

    #[test]
    fn a_bare_type_has_no_name_half() {
        assert_eq!(
            split("vs"),
            SelectorArg {
                type_half: "vs",
                name_half: None,
            }
        );
    }

    // The two acceptance policies are the only difference between the `list`
    // grammar and the `get`/`delete` one, so they are pinned as a pair.

    #[test]
    fn a_bare_type_is_accepted_under_the_allow_policy() {
        assert_matches!(
            ResourceSelectionScanner::scan_selector_arg("vs", BareTypePolicy::Allow),
            Ok(SelectorArg {
                type_half: "vs",
                name_half: None,
            })
        );
    }

    #[test]
    fn a_bare_type_is_rejected_under_the_reject_policy() {
        let error = ResourceSelectionScanner::scan_selector_arg("vs", BareTypePolicy::Reject)
            .expect_err("should be rejected");

        // The caret points just past the type, where the `/` should have been.
        assert_eq!(error.offset, 2);
    }

    #[test]
    fn a_ref_form_argument_is_accepted_under_either_policy() {
        for policy in [BareTypePolicy::Allow, BareTypePolicy::Reject] {
            assert_matches!(
                ResourceSelectionScanner::scan_selector_arg("vs/my-vars", policy),
                Ok(_),
                "expected `vs/my-vars` to be accepted under {policy:?}"
            );
        }
    }

    // Rejected input

    #[test]
    fn rejects_a_second_separator_at_its_own_offset() {
        let error =
            ResourceSelectionScanner::scan_selector_arg("vs/foo/extra", BareTypePolicy::Allow)
                .expect_err("should be rejected");

        assert_eq!(error.offset, 6);
    }

    #[test]
    fn rejects_a_missing_type_half() {
        let error = ResourceSelectionScanner::scan_selector_arg("/my-vars", BareTypePolicy::Allow)
            .expect_err("should be rejected");

        assert_eq!(error.offset, 0);
    }

    #[test]
    fn rejects_a_missing_name_half() {
        let error = ResourceSelectionScanner::scan_selector_arg("vs/", BareTypePolicy::Allow)
            .expect_err("should be rejected");

        assert_eq!(error.offset, 3);
    }

    #[test]
    fn rejects_an_empty_argument() {
        assert_matches!(
            ResourceSelectionScanner::scan_selector_arg("", BareTypePolicy::Allow),
            Err(_)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
