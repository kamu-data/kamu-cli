// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared detection behind the configuration-domain spec lints.
//!
//! `VariableSetSpecInput` and `SecretSetSpecInput` both key entries by name in
//! a `BTreeMap<String, _>` and both store free-form string values, so the
//! *detection* half of several lints is identical between them. The warning
//! codes and messages stay on the spec input types themselves (see
//! `docs/internal/resources-anatomy.md`) — only the checks below are shared.

use std::collections::BTreeMap;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Whether an entry's name and value read as credential material — the
/// detection half of the `secret_material_in_variable` lint.
///
/// A `VariableSet` entry is stored and returned in the clear, unlike a
/// `SecretSet` entry, which is encrypted on apply — so a credential filed
/// here is legal, applies cleanly, and leaks. This type only classifies; it
/// carries no warning code or message, since a credential in a `SecretSet`
/// is exactly right and must not be flagged the same way.
pub struct CredentialShape<'a> {
    name: &'a str,
    value: &'a str,
}

impl<'a> CredentialShape<'a> {
    /// Name suffixes (and bare names) that mark an entry as holding a
    /// credential. Compared case-insensitively: entry names are legally
    /// mixed-case, so `db_password` and `DB_PASSWORD` are equally suspicious.
    const NAME_WORDS: &'static [&'static str] = &[
        "TOKEN",
        "SECRET",
        "PASSWORD",
        "PASSWD",
        "APIKEY",
        "API_KEY",
        "ACCESS_KEY",
        "SECRET_KEY",
        "PRIVATE_KEY",
        "CREDENTIAL",
        "CREDENTIALS",
    ];

    /// This system's own access token prefix
    /// (`kamu_accounts::ACCESS_TOKEN_PREFIX`).
    const KAMU_ACCESS_TOKEN_PREFIX: &'static str = "ka_";

    /// Vendor/self-issued tokens that carry an unambiguous, self-identifying
    /// prefix.
    const KNOWN_PREFIXED_TOKENS: &'static [&'static str] = &[
        Self::KAMU_ACCESS_TOKEN_PREFIX,
        "ghp_",
        "gho_",
        "ghu_",
        "ghs_",
        "ghr_",
        "github_pat_",
        "xoxb-",
        "xoxa-",
        "xoxp-",
        "xoxr-",
        "xoxs-",
    ];

    pub fn new(name: &'a str, value: &'a str) -> Self {
        Self { name, value }
    }

    pub fn is_suspicious(&self) -> bool {
        self.name_looks_like_credential() || self.value_looks_like_credential()
    }

    /// Whether the name equals or ends with `_`-prefixed one of
    /// [`Self::NAME_WORDS`]. The `_` boundary requirement keeps `MYSECRET`
    /// from matching `SECRET`.
    ///
    /// Deliberately excludes bare `KEY`: `PRIMARY_KEY`, `SORT_KEY`,
    /// `OBJECT_KEY`, and `PUBLIC_KEY` are common non-secret names, so only
    /// credential-specific `*_KEY` forms are listed explicitly.
    fn name_looks_like_credential(&self) -> bool {
        let upper = self.name.to_ascii_uppercase();

        Self::NAME_WORDS.iter().any(|word| {
            if upper == *word {
                return true;
            }

            upper
                .len()
                .checked_sub(word.len())
                .is_some_and(|prefix_len| {
                    prefix_len > 0
                        && upper.as_bytes()[prefix_len - 1] == b'_'
                        && &upper[prefix_len..] == *word
                })
        })
    }

    /// Whether the value matches a well-known credential shape. No regex
    /// dependency: each check below is a fixed prefix or character-class
    /// test.
    fn value_looks_like_credential(&self) -> bool {
        self.is_aws_access_key_id()
            || self.is_known_prefixed_token()
            || self.is_pem_private_key()
            || self.is_jwt()
            || self.is_our_own_jwe_secret()
            || self.has_embedded_url_credentials()
    }

    /// AWS access key ID: `AKIA` followed by 16 uppercase alphanumerics.
    fn is_aws_access_key_id(&self) -> bool {
        const AWS_KEY_ID_BODY_LEN: usize = 16;

        let Some(body) = self.value.strip_prefix("AKIA") else {
            return false;
        };

        body.len() == AWS_KEY_ID_BODY_LEN
            && body
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    }

    fn is_known_prefixed_token(&self) -> bool {
        Self::KNOWN_PREFIXED_TOKENS
            .iter()
            .any(|prefix| self.value.len() > prefix.len() && self.value.starts_with(prefix))
    }

    fn is_pem_private_key(&self) -> bool {
        is_pem_private_key(self.value)
    }

    /// A compact JWT: three `.`-separated base64url segments whose header
    /// segment starts with `eyJ` (base64url of `{"`).
    fn is_jwt(&self) -> bool {
        let mut segments = self.value.split('.');

        let (Some(header), Some(payload), Some(signature), None) = (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ) else {
            return false;
        };

        header.starts_with("eyJ")
            && [header, payload, signature]
                .iter()
                .all(|segment| !segment.is_empty() && segment.bytes().all(is_base64url_byte))
    }

    /// A JWE token in the exact compact form `SecretSet` values are encrypted
    /// to on apply — see `crypto_utils::jwe`. This is the shape a value takes
    /// after being copied out of a `SecretSet` and pasted into a `VariableSet`
    /// by mistake, so it must be caught even though it carries no key-derived
    /// signal (it is structural, not a fixed prefix).
    fn is_our_own_jwe_secret(&self) -> bool {
        crypto_utils::jwe::looks_like_compact(self.value)
    }

    /// A URL carrying inline credentials, e.g. `postgres://user:pass@host/db`.
    ///
    /// Requires a non-empty password to avoid flagging `scheme://user@host`,
    /// which leaks a username but no secret.
    fn has_embedded_url_credentials(&self) -> bool {
        let Some((scheme, rest)) = self.value.split_once("://") else {
            return false;
        };

        if scheme.is_empty()
            || !scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'+')
        {
            return false;
        }

        // Authority ends at the first `/`, `?`, or `#`.
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();

        let Some((userinfo, host)) = authority.rsplit_once('@') else {
            return false;
        };

        let Some((user, password)) = userinfo.split_once(':') else {
            return false;
        };

        !user.is_empty() && !password.is_empty() && !host.is_empty()
    }
}

fn is_base64url_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'='
}

/// A PEM-armored key block, in any of its flavors (`RSA`, `EC`, `OPENSSH`,
/// plain `PRIVATE KEY`, `PUBLIC KEY`, `CERTIFICATE`, …).
///
/// Shared between [`CredentialShape`] (a *private*-key block is credential
/// material) and [`ValueShape`] (any PEM block is legitimately multiline, so
/// its embedded newlines must not trip the whitespace lint).
fn is_pem_block(value: &str) -> bool {
    value
        .split("-----BEGIN ")
        .skip(1)
        .any(|rest| rest.contains("-----"))
}

fn is_pem_private_key(value: &str) -> bool {
    value
        .split("-----BEGIN ")
        .skip(1)
        .filter_map(|rest| rest.split_once("-----"))
        .any(|(label, _)| label.ends_with("PRIVATE KEY"))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Whether a single entry's value carries a shape that is legal to store but
/// almost certainly not what the author intended — the detection half of the
/// `suspicious_value_whitespace` and `unexpanded_interpolation` lints.
pub struct ValueShape<'a> {
    value: &'a str,
}

impl<'a> ValueShape<'a> {
    /// Substrings that indicate the author expected templating. These specs
    /// have no interpolation layer, so the value is stored — and used —
    /// literally.
    const INTERPOLATION_MARKERS: &'static [&'static str] = &["${", "$(", "{{"];

    pub fn new(value: &'a str) -> Self {
        Self { value }
    }

    /// Whether the value carries whitespace the author almost certainly did
    /// not intend to store: surrounding whitespace, or an embedded
    /// newline/tab.
    ///
    /// Spec validation only rejects empty and over-long values, so
    /// `" token\n"` round-trips verbatim and silently breaks comparisons
    /// downstream. An all-whitespace value is caught here too — it passes
    /// the `is_empty()` check.
    ///
    /// A PEM block is exempt: its embedded newlines are the format, not an
    /// authoring mistake, and this check would otherwise fire on every apply
    /// of a legitimate multiline key.
    pub fn has_suspicious_whitespace(&self) -> bool {
        if is_pem_block(self.value) {
            return false;
        }

        self.value.trim() != self.value || self.value.contains(['\n', '\r', '\t'])
    }

    /// Whether the value looks like it expected shell/template substitution
    /// that these specs never perform.
    pub fn has_unexpanded_interpolation(&self) -> bool {
        Self::INTERPOLATION_MARKERS
            .iter()
            .any(|marker| self.value.contains(marker))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Entry names that collide under case-insensitive comparison — the
/// detection half of the `case_colliding_names` lint.
///
/// Entries are keyed in a `BTreeMap`, so `DB_HOST` and `db_host` coexist as
/// two distinct entries; anything that folds case on the way out (env-var
/// export on case-insensitive platforms, normalized lookups) sees one of
/// them silently win.
pub struct CaseCollisions<'a> {
    /// Name -> the earlier (in `BTreeMap` order) name it collides with.
    /// Absent for a name that collides with nothing.
    first_by_name: BTreeMap<&'a str, &'a str>,
}

impl<'a> CaseCollisions<'a> {
    /// `BTreeMap` order makes the "earlier" name of a colliding group
    /// deterministic, so [`Self::other`]'s answer is stable.
    pub fn scan<V>(entries: &'a BTreeMap<String, V>) -> Self {
        let mut first_by_folded: BTreeMap<String, &'a str> = BTreeMap::new();
        let mut first_by_name = BTreeMap::new();

        for name in entries.keys() {
            let folded = name.to_ascii_uppercase();

            match first_by_folded.get(&folded) {
                Some(first) => {
                    first_by_name.insert(name.as_str(), *first);
                }
                None => {
                    first_by_folded.insert(folded, name.as_str());
                }
            }
        }

        Self { first_by_name }
    }

    /// The earlier name `name` collides with, if any.
    pub fn other(&self, name: &str) -> Option<&'a str> {
        self.first_by_name.get(name).copied()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn detects_credential_names() {
        for name in [
            "TOKEN",
            "SECRET",
            "API_KEY",
            "ACCESS_KEY",
            "PRIVATE_KEY",
            "AWS_ACCESS_KEY",
            "DB_PASSWORD",
            "db_password",
            "Service_Account_Token",
            "AWS_CREDENTIALS",
        ] {
            assert!(
                CredentialShape::new(name, "irrelevant").is_suspicious(),
                "expected match: {name}"
            );
        }

        for name in [
            "MESSAGE",
            "INPUT_TOPIC",
            "MONKEY",
            "TURKEY",
            "KEYSTORE",
            "PASSWORD_POLICY",
            "Http_Port",
            // Bare `KEY` is deliberately not a credential signal: these are
            // common non-secret names.
            "PRIMARY_KEY",
            "SORT_KEY",
            "OBJECT_KEY",
            "PUBLIC_KEY",
        ] {
            assert!(
                !CredentialShape::new(name, "analytics.events").is_suspicious(),
                "unexpected match: {name}"
            );
        }
    }

    #[test]
    fn detects_credential_values() {
        for value in [
            "AKIAIOSFODNN7EXAMPLE",
            "ka_0123456789ABCDEFGHJKMNPQRS",
            "ghp_16CharactersOfTokenMaterialHere",
            "github_pat_11ABCDEFG0abcdefghij",
            "xoxb-123456789012-abcdefghijkl",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk",
            "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKC\n-----END RSA PRIVATE KEY-----",
            "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaA==\n-----END OPENSSH PRIVATE KEY-----",
            "postgres://user:hunter2@db.example.com:5432/app",
        ] {
            assert!(
                CredentialShape::new("PIPELINE_INPUT", value).is_suspicious(),
                "expected match: {value}"
            );
        }

        for value in [
            "analytics.events",
            "https://example.com/path",
            "https://user@example.com/path",
            "AKIASHORT",
            "AKIAIOSFODNN7EXAMPLE_TOO_LONG",
            "ghp_",
            "not.a.jwt",
            "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----",
        ] {
            assert!(
                !CredentialShape::new("PIPELINE_INPUT", value).is_suspicious(),
                "unexpected match: {value}"
            );
        }
    }

    #[test]
    fn detects_own_jwe_secret_pasted_into_a_variable() {
        // The exact mistake this check exists for: a `SecretSet` value is
        // copied out (already encrypted) and pasted into a `VariableSet`.
        let cryptor =
            crypto_utils::SecretCryptor::try_new("0123456789abcdef0123456789abcdef").unwrap();
        let token = cryptor.encrypt_to_jwe(b"hunter2").unwrap();

        assert!(CredentialShape::new("PIPELINE_INPUT", &token).is_suspicious());
    }

    #[test]
    fn detects_suspicious_whitespace() {
        for value in [
            "  leading",
            "trailing ",
            "with\nnewline",
            "with\ttab",
            "   ",
        ] {
            assert!(
                ValueShape::new(value).has_suspicious_whitespace(),
                "expected match: {value:?}"
            );
        }

        for value in ["clean", "inner spaces are fine", "analytics.events"] {
            assert!(
                !ValueShape::new(value).has_suspicious_whitespace(),
                "unexpected match: {value:?}"
            );
        }
    }

    #[test]
    fn does_not_flag_whitespace_in_a_pem_key() {
        // A PEM block's embedded newlines are the format itself, not an
        // authoring mistake — this must not fire on every apply of a
        // legitimate multiline key.
        let pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKC\n-----END RSA PRIVATE KEY-----";

        assert!(!ValueShape::new(pem).has_suspicious_whitespace());
    }

    #[test]
    fn detects_unexpanded_interpolation() {
        for value in ["${HOME}/data", "$(whoami)", "{{ .Values.host }}"] {
            assert!(
                ValueShape::new(value).has_unexpanded_interpolation(),
                "expected match: {value}"
            );
        }

        for value in ["plain", "$HOME", "100$", "{single}"] {
            assert!(
                !ValueShape::new(value).has_unexpanded_interpolation(),
                "unexpected match: {value}"
            );
        }
    }

    /// A stand-in for a real `variables`/`secrets` entry map, keyed the same
    /// way but with a placeholder value — only the keys matter here.
    fn named_entries(names: &[&str]) -> BTreeMap<String, &'static str> {
        names
            .iter()
            .map(|name| ((*name).to_string(), "value"))
            .collect()
    }

    #[test]
    fn detects_case_colliding_names() {
        let entries = named_entries(&["DB_HOST", "db_host", "MESSAGE"]);
        let collisions = CaseCollisions::scan(&entries);

        // `BTreeMap` orders uppercase before lowercase, so `DB_HOST` is the
        // first of the colliding pair and `db_host` is reported against it.
        assert_eq!(collisions.other("db_host"), Some("DB_HOST"));
        assert_eq!(collisions.other("DB_HOST"), None);
        assert_eq!(collisions.other("MESSAGE"), None);
    }

    #[test]
    fn reports_no_collisions_for_distinct_names() {
        let entries = named_entries(&["DB_HOST", "DB_PORT", "Http_Port"]);
        let collisions = CaseCollisions::scan(&entries);

        assert_eq!(collisions.other("DB_HOST"), None);
        assert_eq!(collisions.other("DB_PORT"), None);
        assert_eq!(collisions.other("Http_Port"), None);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
