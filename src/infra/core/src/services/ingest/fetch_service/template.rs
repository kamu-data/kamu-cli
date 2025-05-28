// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::LazyLock;

use kamu_core::{TemplateError, TemplateInvalidPatternError, TemplateValueNotFoundError};
use regex::Regex;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static RE_ENV: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^env\.([a-zA-Z-_0-9]+)$").expect("Invalid template env variable regex")
});
static RE_NUM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(-?[0-9]+(?:.[0-9]+)?)$").expect("Invalid template number regex")
});
static RE_STR: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^'([^']*)'$").expect("Invalid template string regex"));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum TemplateToken {
    EnvVar(String),
    Literal(String),
}

fn parse_token(token_str: &str) -> Option<TemplateToken> {
    let token_str = token_str.trim();
    if let Some(cap_env) = RE_ENV.captures(token_str) {
        let env_name = cap_env
            .get(1)
            .expect("Missing variable name capture")
            .as_str()
            .trim();
        Some(TemplateToken::EnvVar(env_name.to_string()))
    } else if let Some(cap_num) = RE_NUM.captures(token_str) {
        let number = cap_num
            .get(1)
            .expect("Missing number capture")
            .as_str()
            .trim();
        Some(TemplateToken::Literal(number.to_string()))
    } else if let Some(cap_str) = RE_STR.captures(token_str) {
        let string = cap_str
            .get(1)
            .expect("Missing string capture")
            .as_str()
            .trim();
        Some(TemplateToken::Literal(string.to_string()))
    } else {
        None
    }
}

fn parse_tokens(tokens_str: &str) -> Result<Vec<TemplateToken>, TemplateError> {
    let mut tokens: Vec<TemplateToken> = Vec::new();
    for token_str in tokens_str.split("||") {
        let token_str = token_str.trim();
        match parse_token(token_str) {
            Some(token) => {
                tokens.push(token);
            }
            None => {
                return Err(TemplateError::InvalidPattern(
                    TemplateInvalidPatternError::new(token_str),
                ));
            }
        }
    }
    Ok(tokens)
}

pub fn template_string<'a>(
    s: &'a str,
    lookup_fn: &dyn Fn(&str) -> Option<String>,
) -> Result<String, TemplateError> {
    let mut s = Cow::from(s);
    let re_tpl = Regex::new(r"\$\{\{([^}]*)\}\}").unwrap();
    loop {
        if let Some(ctpl) = re_tpl.captures(&s) {
            let tpl = ctpl.get(0).expect("Missing template capture");
            let tpl_range = tpl.range();

            let tokens_str = ctpl
                .get(1)
                .expect("Missing variable capture")
                .as_str()
                .trim();
            let tokens = parse_tokens(tokens_str)?;

            let value = tokens
                .iter()
                .map(|token| match token {
                    TemplateToken::EnvVar(v) => lookup_fn(v),
                    TemplateToken::Literal(v) => Some(v.clone()),
                })
                .find_map(std::convert::identity);

            match value {
                Some(v) => s.to_mut().replace_range(tpl_range, &v),
                None => {
                    return Err(TemplateError::ValueNotFound(
                        TemplateValueNotFoundError::new(tokens_str),
                    ));
                }
            }
        } else {
            if let Cow::Owned(_) = &s {
                tracing::debug!(%s, "String after template substitution");
            }
            return Ok(s.to_string());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn test_template_string() {
        let lookup_fn = |env_name: &str| -> Option<String> {
            match env_name {
                "test1" => Some(String::from("val1")),
                "test2" => Some(String::from("val2")),
                _ => None,
            }
        };

        // Populates multiple vars.
        let result = template_string(
            "${{ env.test1 }}, ${{ env.test2 || 'default2' }}",
            &lookup_fn,
        );
        assert_matches!(result, Ok(val) if val.as_str() == "val1, val2");

        // When at least one of vars is missing, returns error.
        let result = template_string("${{ env.test1 }}, ${{ env.missing_var }}", &lookup_fn);
        assert_matches!(result, Err(TemplateError::ValueNotFound(err))
            if err.template.as_str() == "env.missing_var");

        // Default value is being used if present.
        let result = template_string("foo=${{ env.test3 || 'default-val3' }}", &lookup_fn);
        assert_matches!(result, Ok(val) if val.as_str() == "foo=default-val3");
    }

    #[test]
    fn test_template_string_numbers() {
        let lookup_fn = |_: &str| -> Option<String> { None };

        assert_matches!(
            template_string("${{ env.test2 || 123 }}", &lookup_fn),
            Ok(val) if val.as_str() == "123");

        assert_matches!(
            template_string("${{ env.test2 || -123 }}", &lookup_fn),
            Ok(val) if val.as_str() == "-123");

        assert_matches!(
            template_string("${{ env.test2 || 0.5 }}", &lookup_fn),
            Ok(val) if val.as_str() == "0.5");

        assert_matches!(
            template_string("X=${{ env.test2 || qwe1 }}", &lookup_fn),
            Err(TemplateError::InvalidPattern(err))
                if err.pattern.as_str() == "qwe1");
    }

    #[test]
    fn test_template_string_strings() {
        let lookup_fn = |_: &str| -> Option<String> { None };

        let result = template_string("${{ env.test2 || '\"random str __%.?' }}\"", &lookup_fn);
        assert_matches!(result, Ok(val) if val.as_str() == "\"random str __%.?\"");
    }

    #[test]
    fn test_parse_token() {
        assert_matches!(
            parse_token("env.some_var1"),
            Some(TemplateToken::EnvVar(var)) if var == "some_var1");

        assert_matches!(
            parse_token("10"),
            Some(TemplateToken::Literal(var)) if var == "10");
        assert_matches!(
            parse_token("-10.5"),
            Some(TemplateToken::Literal(var)) if var == "-10.5");

        assert_matches!(
            parse_token(r###"'"some $tr1ng!"'"###),
            Some(TemplateToken::Literal(var)) if var == "\"some $tr1ng!\"");

        assert_matches!(parse_token("unsupported_var"), None);
    }

    #[test]
    fn test_parse_tokens() {
        let pattern = "env.var1 || env.var2 || 5";
        assert_matches!(parse_tokens(pattern), Ok(_));

        let result = parse_tokens(pattern).ok().unwrap();
        assert_matches!(
            result.first().unwrap(),
            TemplateToken::EnvVar(v) if v.as_str() == "var1");
        assert_matches!(
            result.get(1).unwrap(),
            TemplateToken::EnvVar(v) if v.as_str() == "var2");
        assert_matches!(
            result.get(2).unwrap(),
            TemplateToken::Literal(v) if v.as_str() == "5");

        assert_matches!(
            parse_tokens("env.var1 || env.var2 || broken_template"),
            Err(TemplateError::InvalidPattern(err))
                if err.pattern.as_str() == "broken_template");
    }

    #[test]
    fn test_template_string_multiple_options() {
        let lookup_fn = |env_name: &str| -> Option<String> {
            match env_name {
                "test1" => Some(String::from("val1")),
                _ => None,
            }
        };

        let result = template_string(
            "foo=${{ env.test3 || env.test4 || 'default-val3' }}",
            &lookup_fn,
        );
        assert_matches!(result, Ok(val) if val.as_str() == "foo=default-val3");

        let result = template_string(
            "foo=${{ env.test3 || env.test1 || 'default-val3' }}",
            &lookup_fn,
        );
        assert_matches!(result, Ok(val) if val.as_str() == "foo=val1");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
