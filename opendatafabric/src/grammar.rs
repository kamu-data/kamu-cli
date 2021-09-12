// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub struct DatasetIDGrammar;

impl DatasetIDGrammar {
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

    fn match_char(s: &str, c: char) -> Option<(&str, &str)> {
        if s.len() > 0 && s.as_bytes()[0] == (c as u8) {
            Some((&s[0..1], &s[1..s.len()]))
        } else {
            None
        }
    }

    fn match_alphanums(s: &str) -> Option<(&str, &str)> {
        let alnums = s.bytes().take_while(|b| b.is_ascii_alphanumeric()).count();

        if alnums == 0 {
            None
        } else {
            Some((&s[0..alnums], &s[alnums..s.len()]))
        }
    }

    fn match_subdomain(s: &str) -> Option<(&str, &str)> {
        let (h, t) = Self::match_alphanums(s)?;

        let (hh, tt) = Self::match_zero_or_many(t, |s| {
            let (_, t) = Self::match_char(s, '-')?;
            let (h, tt) = Self::match_alphanums(t)?;
            Some((&s[0..h.len() + 1], tt))
        })?;

        Some((&s[0..h.len() + hh.len()], tt))
    }

    pub fn match_hostname(s: &str) -> Option<(&str, &str)> {
        let (h, t) = Self::match_subdomain(s)?;

        let (hh, tt) = Self::match_zero_or_many(t, |s| {
            let (_, t) = Self::match_char(s, '.')?;
            let (h, tt) = Self::match_subdomain(t)?;
            Some((&s[0..h.len() + 1], tt))
        })?;

        Some((&s[0..h.len() + hh.len()], tt))
    }

    pub fn match_dataset_id(s: &str) -> Option<(&str, &str)> {
        Self::match_hostname(s)
    }

    pub fn match_username(s: &str) -> Option<(&str, &str)> {
        Self::match_subdomain(s)
    }

    pub fn match_repository(s: &str) -> Option<(&str, &str)> {
        Self::match_hostname(s)
    }

    /// See: https://github.com/kamu-data/open-data-fabric/blob/master/open-data-fabric.md#dataset-identity
    pub fn match_dataset_ref(s: &str) -> Option<(&str, &str)> {
        // TODO: Should not be eagerly counting?
        let seps = s.chars().filter(|c| *c == '/').count();
        match seps {
            0 => Self::match_dataset_id(s),
            1 => {
                let (rh, rt) = Self::match_repository(s)?;
                let (_, st) = Self::match_char(rt, '/')?;
                let (ih, it) = Self::match_dataset_id(st)?;
                Some((&s[0..rh.len() + 1 + ih.len()], it))
            }
            2 => {
                let (rh, rt) = Self::match_repository(s)?;
                let (_, s1t) = Self::match_char(rt, '/')?;
                let (uh, ut) = Self::match_username(s1t)?;
                let (_, s2t) = Self::match_char(ut, '/')?;
                let (ih, it) = Self::match_dataset_id(s2t)?;
                Some((&s[0..rh.len() + 1 + uh.len() + 1 + ih.len()], it))
            }
            _ => None,
        }
    }
}
