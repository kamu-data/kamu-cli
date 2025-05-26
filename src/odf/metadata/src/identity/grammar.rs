// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::DID_ODF_PREFIX;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Grammar;

/// See: <https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataset-identity>
impl Grammar {
    fn match_non_eof(s: &str) -> Option<(&str, &str)> {
        if s.is_empty() {
            None
        } else {
            Some(("", s))
        }
    }

    fn match_zero_or_many(
        s: &str,
        matcher: impl Fn(&str) -> Option<(&str, &str)>,
    ) -> Option<(&str, &str)> {
        let mut len = 0;
        let mut tail = s;
        loop {
            match matcher(tail) {
                Some((head, ntail)) => {
                    len += head.len();
                    tail = ntail;
                }
                _ => break Some((&s[0..len], &s[len..s.len()])),
            }
        }
    }

    fn match_one_or_many(
        s: &str,
        matcher: impl Fn(&str) -> Option<(&str, &str)>,
    ) -> Option<(&str, &str)> {
        let (h, t) = matcher(s)?;
        let (hh, tt) = Self::match_zero_or_many(t, matcher)?;
        Some((&s[0..h.len() + hh.len()], tt))
    }

    fn match_alt(
        s: &str,
        matcher1: impl Fn(&str) -> Option<(&str, &str)>,
        matcher2: impl Fn(&str) -> Option<(&str, &str)>,
    ) -> Option<(&str, &str)> {
        matcher1(s).or_else(|| matcher2(s))
    }

    fn match_opt(s: &str, matcher: impl Fn(&str) -> Option<(&str, &str)>) -> Option<(&str, &str)> {
        matcher(s).or(Some(("", s)))
    }

    fn match_char(s: &str, c: char) -> Option<(&str, &str)> {
        if !s.is_empty() && s.as_bytes()[0] == (c as u8) {
            Some((&s[0..1], &s[1..s.len()]))
        } else {
            None
        }
    }

    fn match_one_of_chars<'a>(s: &'a str, chars: &[char]) -> Option<(&'a str, &'a str)> {
        if !s.is_empty() {
            let c = s.as_bytes()[0] as char;
            if chars.contains(&c) {
                return Some((&s[0..1], &s[1..s.len()]));
            }
        }
        None
    }

    fn match_str<'a>(s: &'a str, prefix: &str) -> Option<(&'a str, &'a str)> {
        s.strip_prefix(prefix).map(|end| (&s[0..prefix.len()], end))
    }

    fn match_alphanums(s: &str) -> Option<(&str, &str)> {
        let alnums = s.bytes().take_while(u8::is_ascii_alphanumeric).count();

        if alnums == 0 {
            None
        } else {
            Some((&s[0..alnums], &s[alnums..]))
        }
    }

    fn match_predicate(s: &str, pred: impl FnMut(&u8) -> bool) -> Option<(&str, &str)> {
        let matched = s.bytes().take_while(pred).count();
        if matched == 0 {
            None
        } else {
            Some((&s[0..matched], &s[matched..]))
        }
    }

    #[allow(dead_code)]
    // Multibase = [a-zA-Z0-9+/=]+
    fn match_multibase(s: &str) -> Option<(&str, &str)> {
        Self::match_predicate(
            s,
            |b| matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'+' | b'/' | b'='),
        )
    }

    fn match_scheme(s: &str) -> Option<(&str, &str)> {
        let (h, t) =
            Self::match_predicate(s, |b| matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9'))?;

        let (hh, tt) = Self::match_zero_or_many(t, |s| {
            let (_, t) = Self::match_char(s, '+')?;
            let (h, tt) =
                Self::match_predicate(t, |b| matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9'))?;
            Some((&s[0..=h.len()], tt))
        })?;

        Some((&s[0..h.len() + hh.len()], tt))
    }

    pub fn match_url(s: &str) -> Option<(&str, &str)> {
        let (scheme, t) = Self::match_scheme(s)?;
        let (_, _) = Self::match_str(t, "://")?;
        Some((scheme, s))
    }

    // Subdomain = [a-zA-Z0-9]+ ("-" [a-zA-Z0-9]+)*
    fn match_subdomain(s: &str) -> Option<(&str, &str)> {
        let (h, t) = Self::match_alphanums(s)?;

        let (hh, tt) = Self::match_zero_or_many(t, |s| {
            let (_, t) = Self::match_char(s, '-')?;
            let (h, tt) = Self::match_alphanums(t)?;
            Some((&s[0..=h.len()], tt))
        })?;

        Some((&s[0..h.len() + hh.len()], tt))
    }

    // Hostname = Subdomain ("." Subdomain)*
    pub fn match_hostname(s: &str) -> Option<(&str, &str)> {
        let (h, t) = Self::match_subdomain(s)?;

        let (hh, tt) = Self::match_zero_or_many(t, |s| {
            let (_, t) = Self::match_char(s, '.')?;
            let (h, tt) = Self::match_subdomain(t)?;
            Some((&s[0..=h.len()], tt))
        })?;

        Some((&s[0..h.len() + hh.len()], tt))
    }

    // StartElement = HostName ("." | "-")?
    pub fn match_hostname_pattern_start_element(s: &str) -> Option<(&str, &str)> {
        let (h1, t1) = Self::match_hostname(s)?;
        let (h2, t2) = Self::match_opt(t1, |s| Self::match_one_of_chars(s, &['.', '-']))?;
        Some((&s[..h1.len() + h2.len()], t2))
    }

    // MiddleElement = (
    //     (("." | "-")? HostName ("." | "-")?)
    //   | ("." | "-")
    // ) non-EOF
    pub fn match_hostname_pattern_middle_element(s: &str) -> Option<(&str, &str)> {
        let (h1, t1) = Self::match_alt(
            s,
            |s| {
                let (h1, t1) = Self::match_opt(s, |s| Self::match_one_of_chars(s, &['.', '-']))?;
                let (h2, t2) = Self::match_hostname(t1)?;
                let (h3, t3) = Self::match_opt(t2, |s| Self::match_one_of_chars(s, &['.', '-']))?;
                Some((&s[..h1.len() + h2.len() + h3.len()], t3))
            },
            |s| Self::match_one_of_chars(s, &['.', '-']),
        )?;
        let (h2, t2) = Self::match_non_eof(t1)?;
        Some((&s[..h1.len() + h2.len()], t2))
    }

    // EndElement = ("." | "-")? HostName
    pub fn match_hostname_pattern_end_element(s: &str) -> Option<(&str, &str)> {
        let (h1, t1) = Self::match_opt(s, |s| Self::match_one_of_chars(s, &['.', '-']))?;
        let (h2, t2) = Self::match_hostname(t1)?;
        Some((&s[..h1.len() + h2.len()], t2))
    }

    // Pattern = StartElement? ("%"+ MiddleElement)* "%"+ EndElement?
    pub fn match_hostname_pattern(s: &str) -> Option<(&str, &str)> {
        // StartElement?
        let (h1, t1) = Self::match_opt(s, Self::match_hostname_pattern_start_element)?;

        // ("%"+ MiddleElement)*
        let (h2, t2) = Self::match_zero_or_many(t1, |s| {
            let (h1, t1) = Self::match_one_or_many(s, |s| Self::match_char(s, '%'))?;
            let (h2, t2) = Self::match_hostname_pattern_middle_element(t1)?;
            Some((&s[..h1.len() + h2.len()], t2))
        })?;

        // "%"+
        let (h3, t3) = Self::match_one_or_many(t2, |s| Self::match_char(s, '%'))?;

        // EndElement?
        let (h4, t4) = Self::match_opt(t3, Self::match_hostname_pattern_end_element)?;

        Some((&s[..h1.len() + h2.len() + h3.len() + h4.len()], t4))
    }

    // DatasetID = "did:odf:" Multibase
    pub fn match_dataset_id(s: &str) -> Option<(&str, &str)> {
        let (h, t) = Self::match_str(s, DID_ODF_PREFIX)?;
        let (hh, tt) = Self::match_multibase(t)?;
        Some((&s[..h.len() + hh.len()], tt))
    }

    // DatasetName = Hostname
    pub fn match_dataset_name(s: &str) -> Option<(&str, &str)> {
        Self::match_hostname(s)
    }

    // DatasetNamePattern = Hostname with possible wildcard symbols
    pub fn match_dataset_name_pattern(s: &str) -> Option<(&str, &str)> {
        // Try parsing as pattern (with wildcards) or a host name (without)
        Self::match_alt(s, Self::match_hostname_pattern, Self::match_hostname)
    }

    // AccountName = Hostname
    pub fn match_account_name(s: &str) -> Option<(&str, &str)> {
        Self::match_hostname(s)
    }

    // RepoName = Hostname
    pub fn match_repo_name(s: &str) -> Option<(&str, &str)> {
        Self::match_hostname(s)
    }

    // (RepoName "/")? DatasetID
    pub fn match_remote_dataset_id(s: &str) -> Option<(Option<&str>, &str, &str)> {
        match s.split_once('/') {
            None => {
                let (id, tail) = Self::match_dataset_id(s)?;
                Some((None, id, tail))
            }
            Some((head, tail)) => match Self::match_repo_name(head) {
                Some((repo, "")) => {
                    let (id, tail) = Self::match_dataset_id(tail)?;
                    Some((Some(repo), id, tail))
                }
                _ => None,
            },
        }
    }

    // DatasetAlias = (AccountName "/")? DatasetName
    pub fn match_dataset_alias(s: &str) -> Option<(Option<&str>, &str, &str)> {
        match s.split_once('/') {
            None => {
                let (ds, tail) = Self::match_dataset_name(s)?;
                Some((None, ds, tail))
            }
            Some((acc, ds)) => match Self::match_account_name(acc) {
                Some((acc, "")) => {
                    let (ds, tail) = Self::match_dataset_name(ds)?;
                    Some((Some(acc), ds, tail))
                }
                _ => None,
            },
        }
    }

    // DatasetAliasRemote = RepoName "/" (AccountName "/")? DatasetName
    pub fn match_dataset_alias_remote(s: &str) -> Option<(&str, Option<&str>, &str, &str)> {
        match s.split_once('/') {
            None => None,
            Some((repo, rest)) => match Self::match_repo_name(repo) {
                Some((repo, "")) => match rest.split_once('/') {
                    Some((acc, rest)) => match Self::match_account_name(acc) {
                        Some((acc, "")) => {
                            let (ds, tail) = Self::match_dataset_name(rest)?;
                            Some((repo, Some(acc), ds, tail))
                        }
                        _ => None,
                    },
                    None => {
                        let (ds, tail) = Self::match_dataset_name(rest)?;
                        Some((repo, None, ds, tail))
                    }
                },
                _ => None,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
