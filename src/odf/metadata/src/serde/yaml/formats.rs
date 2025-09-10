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

pub mod base64 {
    use ::base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        let s = ::base64::engine::general_purpose::STANDARD.encode(data);
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        ::base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

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

/// This is needed to override the `serde_json`'s special handling of arbitrary
/// precision types. Although this crate does not enable `arbitrary_precision`
/// feature - it can get enabled by other dependencies and will result in a
/// special `$serde_json::private::Number` wrapper type emitted for numbers when
/// serializing into YAML even though it also supports arbitrary precision
/// numbers.
///
/// We use the fact that JSON is a valid subset of YAML and:
/// - serialize data to JSON
/// - deserialize it into `serde_yaml::Value` which doesn't handle arbitrary
///   precision
/// - serialize it again into desired format
///
/// This is obviously horrible. Solving this correctly likely requires arbitrary
/// number precision support on `serde` level.
pub mod map_value_limited_precision {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        value: &serde_json::Map<String, serde_json::Value>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let s = serde_json::to_string(value).unwrap();
        let yaml: serde_yaml::Value = serde_yaml::from_str(&s).unwrap();
        yaml.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<serde_json::Map<String, serde_json::Value>, D::Error> {
        serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)
    }
}
