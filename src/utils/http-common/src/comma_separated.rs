// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CommaSeparatedSet<T>(BTreeSet<T>);

impl<T> CommaSeparatedSet<T> {
    pub fn new() -> Self {
        Self(BTreeSet::new())
    }
}

impl<T> From<CommaSeparatedSet<T>> for BTreeSet<T> {
    fn from(value: CommaSeparatedSet<T>) -> Self {
        value.0
    }
}

impl<T: Ord, const N: usize> From<[T; N]> for CommaSeparatedSet<T> {
    fn from(value: [T; N]) -> Self {
        Self(BTreeSet::from(value))
    }
}

impl<T: std::fmt::Display> serde::Serialize for CommaSeparatedSet<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use std::io::Write as _;

        let mut buf = Vec::new();
        let mut first = true;
        for v in &self.0 {
            if !first {
                buf.push(b',');
            } else {
                first = false;
            }
            write!(&mut buf, "{v}").unwrap();
        }

        serializer.collect_str(std::str::from_utf8(&buf).unwrap())
    }
}

impl<T: Ord> std::ops::Deref for CommaSeparatedSet<T> {
    type Target = BTreeSet<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'de, T> serde::Deserialize<'de> for CommaSeparatedSet<T>
where
    T: Ord,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let sequence = String::deserialize(deserializer)?;
        if sequence.is_empty() {
            return Ok(CommaSeparatedSet::new());
        }
        let mut set = BTreeSet::new();
        for elem in sequence.split(',') {
            let val = elem.parse().map_err(serde::de::Error::custom)?;
            set.insert(val);
        }
        Ok(CommaSeparatedSet(set))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn comma_separated_set_empty() {
        assert_eq!(
            serde_json::to_value(CommaSeparatedSet::<String>::from([])).unwrap(),
            json!(""),
        );

        assert_eq!(
            serde_json::from_value::<CommaSeparatedSet::<String>>(json!("")).unwrap(),
            CommaSeparatedSet::from([]),
        );
    }

    #[test]
    fn comma_separated_set_duplicates() {
        assert_eq!(
            serde_json::to_value(CommaSeparatedSet::<String>::from([
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "b".to_string()
            ]))
            .unwrap(),
            json!("a,b,c"),
        );

        assert_eq!(
            serde_json::from_value::<CommaSeparatedSet::<String>>(json!("a,b,b,c")).unwrap(),
            CommaSeparatedSet::from(["a".to_string(), "b".to_string(), "c".to_string()]),
        );
    }

    #[test]
    fn comma_separated_set_enum() {
        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, strum::Display, strum::EnumString)]
        enum X {
            #[strum(serialize = "foo", serialize = "Foo")]
            Foo,
            #[strum(serialize = "bar", serialize = "Bar")]
            Bar,
            #[strum(serialize = "baz", serialize = "Baz")]
            Baz,
        }

        assert_eq!(
            serde_json::to_value(CommaSeparatedSet::from([X::Foo, X::Bar, X::Foo])).unwrap(),
            json!("Foo,Bar"),
        );

        assert_eq!(
            serde_json::from_value::<CommaSeparatedSet::<X>>(json!("Foo,foo,bar")).unwrap(),
            CommaSeparatedSet::from([X::Foo, X::Bar]),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
