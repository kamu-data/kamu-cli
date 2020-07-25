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

    pub fn match_dataset_id(s: &str) -> Option<(&str, &str)> {
        let (h, t) = Self::match_subdomain(s)?;

        let (hh, tt) = Self::match_zero_or_many(t, |s| {
            let (_, t) = Self::match_char(s, '.')?;
            let (h, tt) = Self::match_subdomain(t)?;
            Some((&s[0..h.len() + 1], tt))
        })?;

        Some((&s[0..h.len() + hh.len()], tt))
    }
}
