// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: Yep... all this to serde an Option<DateTime> in a slightly different
// format. See: https://github.com/serde-rs/serde/issues/723

use serde::{Deserialize, Deserializer, Serialize, Serializer};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! impl_struct_or_string {
    ($proxy:path, $dto:path) => {
        impl std::str::FromStr for $proxy {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let v = <$dto>::try_from(s).map_err(|e| e.to_string())?;
                Ok(v.into())
            }
        }

        impl From<$dto> for StructOrString<$proxy> {
            fn from(v: $dto) -> Self {
                Self(v.into())
            }
        }

        impl TryFrom<StructOrString<$proxy>> for $dto {
            type Error = ValidationError;
            fn try_from(v: StructOrString<$proxy>) -> Result<Self, Self::Error> {
                v.0.try_into()
            }
        }
    };
}

pub(crate) use impl_struct_or_string;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct StructOrString<T>(pub T);

impl<T> std::fmt::Debug for StructOrString<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> From<T> for StructOrString<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> Serialize for StructOrString<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        T::serialize(&self.0, serializer)
    }
}

impl<'de, T> Deserialize<'de> for StructOrString<T>
where
    T: Deserialize<'de>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let inner =
            deserializer.deserialize_any(StructOrStringVisitor::<T>(std::marker::PhantomData))?;

        Ok(Self(inner))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct StructOrStringVisitor<T>(std::marker::PhantomData<fn() -> T>);

impl<'de, T> serde::de::Visitor<'de> for StructOrStringVisitor<T>
where
    T: Deserialize<'de>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    type Value = T;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Struct or string")
    }

    fn visit_str<E>(self, value: &str) -> Result<T, E>
    where
        E: serde::de::Error,
    {
        match T::from_str(value) {
            Ok(v) => Ok(v),
            Err(e) => Err(serde::de::Error::custom(e)),
        }
    }

    fn visit_map<M>(self, map: M) -> Result<T, M::Error>
    where
        M: serde::de::MapAccess<'de>,
    {
        Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod base64 {
    use ::base64::Engine;
    use ::base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        let s = BASE64_STANDARD.encode(data);
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        BASE64_STANDARD
            .decode(s)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod base64_opt {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        option: &Option<Vec<u8>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match option {
            None => serializer.serialize_none(),
            Some(date) => super::base64::serialize(date, serializer),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Vec<u8>>, D::Error> {
        super::base64::deserialize(deserializer).map(Some)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod datetime_rfc3339 {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        date: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let s = date.to_rfc3339_opts(SecondsFormat::AutoSi, true);
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error> {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&s)
            .map(Into::into)
            .map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod datetime_rfc3339_opt {
    use chrono::{DateTime, Utc};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        option: &Option<DateTime<Utc>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match option {
            None => serializer.serialize_none(),
            Some(date) => super::datetime_rfc3339::serialize(date, serializer),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<DateTime<Utc>>, D::Error> {
        super::datetime_rfc3339::deserialize(deserializer).map(Some)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This is needed to override the `serde_json`'s special handling of arbitrary
/// precision types. Although this crate does not enable `arbitrary_precision`
/// feature - it can get enabled by other dependencies and will result in a
/// special `$serde_json::private::Number` wrapper type emitted for numbers when
/// serializing into YAML even though it also supports arbitrary precision
/// numbers.
///
/// We use the fact that JSON is a valid subset of YAML and:
/// - serialize data to JSON
/// - deserialize it into `serde_yaml::Value` which doesn't handle arbitrary precision
/// - serialize it again into desired format
///
/// This is obviously horrible. Solving this correctly likely requires arbitrary
/// number precision support on `serde` level.
pub mod map_value_limited_precision {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer, K: Serialize>(
        value: &std::collections::BTreeMap<K, serde_json::Value>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let s = serde_json::to_string(value).unwrap();
        let yaml: serde_yaml::Value = serde_yaml::from_str(&s).unwrap();
        yaml.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>, K>(
        deserializer: D,
    ) -> Result<std::collections::BTreeMap<K, serde_json::Value>, D::Error>
    where
        K: Deserialize<'de> + std::cmp::Ord,
    {
        std::collections::BTreeMap::<K, serde_json::Value>::deserialize(deserializer)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
