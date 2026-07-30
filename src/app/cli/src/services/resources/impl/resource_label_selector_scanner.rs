// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use winnow::combinator::{alt, eof, preceded, repeat, terminated};
use winnow::error::{ErrMode, StrContext, StrContextValue};
use winnow::token::{one_of, take_while};
use winnow::{ModalResult, Parser};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Lexical structure
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Separates predicates within one conjunction.
pub const PREDICATE_SEPARATOR: char = ',';

/// Separates a key from its value inside one predicate.
pub const EQUALS: char = '=';

/// Introduces an escape sequence.
pub const ESCAPE: char = '\\';

/// Characters an escape may legally precede, beyond the reserved sigils.
/// Anything else is rejected rather than silently passed through, so adding a
/// future escape (`\n`, `\uXXXX`) cannot change the meaning of input that
/// scans today.
pub const ESCAPABLE: [char; 3] = [PREDICATE_SEPARATOR, EQUALS, ESCAPE];

/// Sigils reserved for boolean combinators (`$not`, `$or`) and their grouping.
/// Rejected in bare words so that the eventual `$or(a=1, b=2)` syntax does not
/// collide with keys or values accepted today. Escape them to use them
/// literally.
pub const RESERVED: [char; 4] = ['$', '(', ')', '!'];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One lexical token. Whitespace never reaches this level — the scanner drops
/// it between tokens, so the grammar never mentions it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    /// A run of text with escapes already resolved.
    Word(String),
    Equals,
    Separator,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A token plus its byte span in the original input, kept so grammar errors can
/// be reported against the text the user typed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spanned {
    pub token: Token,
    pub start: usize,
    pub end: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Failure to tokenize, carrying the offset the grammar layer renders a caret
/// against.
#[derive(Debug)]
pub struct ScanError {
    pub offset: usize,
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Turns a raw `--label` argument into tokens, resolving escapes and dropping
/// insignificant whitespace so the grammar never has to mention either.
pub struct ResourceLabelSelectorScanner;

impl ResourceLabelSelectorScanner {
    pub fn scan(input: &str) -> Result<Vec<Spanned>, ScanError> {
        scanner.parse(input).map_err(|err| ScanError {
            offset: err.offset(),
            message: err.inner().to_string(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Productions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The only production that mentions whitespace.
fn scanner(input: &mut &str) -> ModalResult<Vec<Spanned>> {
    let total_len = input.len();

    terminated(
        preceded(
            whitespace,
            repeat(0.., terminated(spanned_token(total_len), whitespace)),
        ),
        eof,
    )
    .parse_next(input)
}

/// Wraps [`token`] to record its span. Offsets are derived from how much input
/// remains, which keeps the scanner working on a plain `&str` instead of
/// requiring a `Located` stream just to carry positions.
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
        one_of(EQUALS).value(Token::Equals),
        one_of(PREDICATE_SEPARATOR).value(Token::Separator),
        word.map(Token::Word),
    ))
    .parse_next(input)
}

/// A run of plain characters and escapes, with inner whitespace preserved but
/// trailing whitespace left for the scanner to skip.
fn word(input: &mut &str) -> ModalResult<String> {
    repeat(
        1..,
        alt((
            plain_run.map(Fragment::Plain),
            escape.map(Fragment::Escaped),
        )),
    )
    .fold(String::new, |mut acc: String, fragment| {
        match fragment {
            Fragment::Plain(text) => acc.push_str(text),
            Fragment::Escaped(c) => acc.push(c),
        }
        acc
    })
    .map(|word| word.trim_end().to_string())
    .parse_next(input)
}

/// A single piece of a word: either a verbatim run or one resolved escape.
enum Fragment<'i> {
    Plain(&'i str),
    Escaped(char),
}

/// Any run of characters that is not a delimiter, an escape, or a reserved
/// sigil. Inner whitespace is included so `owner=data platform` keeps its
/// space; the trailing run is trimmed by [`word`].
fn plain_run<'i>(input: &mut &'i str) -> ModalResult<&'i str> {
    take_while(1.., |c: char| {
        !matches!(c, PREDICATE_SEPARATOR | EQUALS | ESCAPE) && !RESERVED.contains(&c)
    })
    .parse_next(input)
}

/// `escape = "\" , ( "," | "=" | "\" | reserved )`
fn escape(input: &mut &str) -> ModalResult<char> {
    // Absence of a backslash is an ordinary backtrack — the word simply has no
    // further fragments. Only once the backslash has been consumed does an
    // invalid continuation become fatal, so `cut` applies to the tail alone.
    let _ = one_of(ESCAPE).parse_next(input)?;

    alt((one_of(ESCAPABLE), one_of(RESERVED)))
        .context(StrContext::Expected(StrContextValue::Description(
            r"one of `\,` `\=` `\\` after a backslash",
        )))
        .parse_next(input)
        .map_err(ErrMode::cut)
}

fn whitespace<'i>(input: &mut &'i str) -> ModalResult<&'i str> {
    take_while(0.., [' ', '\t']).parse_next(input)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn scan(input: &str) -> Vec<Token> {
        ResourceLabelSelectorScanner::scan(input)
            .expect("input should scan")
            .into_iter()
            .map(|spanned| spanned.token)
            .collect()
    }

    fn word(text: &str) -> Token {
        Token::Word(text.to_string())
    }

    #[test]
    fn drops_whitespace_between_tokens() {
        assert_eq!(
            scan("  env = prod , tier = backend "),
            vec![
                word("env"),
                Token::Equals,
                word("prod"),
                Token::Separator,
                word("tier"),
                Token::Equals,
                word("backend"),
            ]
        );
    }

    #[test]
    fn preserves_whitespace_inside_a_word() {
        assert_eq!(scan("a b"), vec![word("a b")]);
    }

    #[test]
    fn resolves_escapes_into_word_text() {
        assert_eq!(scan(r"a\,b"), vec![word("a,b")]);
        assert_eq!(scan(r"a\=b"), vec![word("a=b")]);
        assert_eq!(scan(r"a\\b"), vec![word(r"a\b")]);
    }

    #[test]
    fn resolves_escaped_reserved_sigils() {
        assert_eq!(scan(r"cost\$center"), vec![word("cost$center")]);
    }

    #[test]
    fn yields_no_tokens_for_blank_input() {
        assert!(scan("   ").is_empty());
    }

    #[test]
    fn records_spans_against_the_original_input() {
        let tokens = ResourceLabelSelectorScanner::scan("env = prod").expect("should scan");

        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[1].token, Token::Equals);
        assert_eq!(tokens[1].start, 4);
        assert_eq!(tokens[1].end, 5);
        assert_eq!(tokens[2].start, 6);
    }

    #[test]
    fn a_word_span_covers_the_whitespace_it_consumed() {
        // `word` consumes the trailing run and only then trims it, so the span
        // end sits past the visible text. Carets are rendered against token
        // starts, so this never mislocates an error.
        let tokens = ResourceLabelSelectorScanner::scan("env  =x").expect("should scan");

        assert_eq!(tokens[0].token, word("env"));
        assert_eq!(tokens[0].start, 0);
        assert_eq!(tokens[0].end, 5);
    }

    #[test]
    fn rejects_an_unknown_escape() {
        let error = ResourceLabelSelectorScanner::scan(r"a\nb").expect_err("should be rejected");

        assert!(
            error.message.contains("after a backslash"),
            "unexpected message: {}",
            error.message
        );
    }

    #[test]
    fn rejects_a_trailing_backslash() {
        assert!(ResourceLabelSelectorScanner::scan(r"value\").is_err());
    }

    #[test]
    fn rejects_bare_reserved_sigils() {
        for reserved in ["$or", "a(b", "a)b", "a!b"] {
            assert!(
                ResourceLabelSelectorScanner::scan(reserved).is_err(),
                "expected `{reserved}` to be rejected as reserved syntax"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
