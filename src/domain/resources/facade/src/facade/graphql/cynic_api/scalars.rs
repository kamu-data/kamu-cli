// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_resources as domain;

use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

cynic::impl_scalar!(DateTime<Utc>, schema::DateTime);
cynic::impl_scalar!(odf::AccountID, schema::AccountID);
cynic::impl_scalar!(domain::ResourceID, schema::ResourceID2);
cynic::impl_scalar!(serde_json::Value, schema::JSON);
cynic::impl_scalar!(Uint64, schema::Uint64);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Newtype wrapper for `u64` that (de)serializes as a JSON **string** to match
/// the server-side `UInt64` scalar, which encodes 64-bit integers as strings to
/// avoid JavaScript precision loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Uint64(pub(crate) u64);

impl From<Uint64> for u64 {
    fn from(value: Uint64) -> Self {
        value.0
    }
}

impl serde::Serialize for Uint64 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Uint64 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = Uint64;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a u64 value encoded as a JSON string")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                v.parse::<u64>().map(Uint64).map_err(E::custom)
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Uint64(v))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::Scalar, Debug, Clone)]
#[cynic(graphql_type = "AccountName")]
pub(crate) struct AccountName(pub(crate) String);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint64_deserializes_from_string() {
        let v: Uint64 = serde_json::from_str(r#""42""#).unwrap();
        assert_eq!(v.0, 42);
    }

    #[test]
    fn uint64_deserializes_from_integer() {
        let v: Uint64 = serde_json::from_str("42").unwrap();
        assert_eq!(v.0, 42);
    }

    #[test]
    fn uint64_deserializes_zero() {
        assert_eq!(serde_json::from_str::<Uint64>(r#""0""#).unwrap().0, 0);
        assert_eq!(serde_json::from_str::<Uint64>("0").unwrap().0, 0);
    }

    #[test]
    fn uint64_deserializes_max() {
        let max = u64::MAX.to_string();
        let v: Uint64 = serde_json::from_str(&format!(r#""{max}""#)).unwrap();
        assert_eq!(v.0, u64::MAX);
    }

    #[test]
    fn uint64_serializes_as_string() {
        assert_eq!(serde_json::to_string(&Uint64(1)).unwrap(), r#""1""#);
        assert_eq!(
            serde_json::to_string(&Uint64(u64::MAX)).unwrap(),
            format!(r#""{}""#, u64::MAX)
        );
    }

    #[test]
    fn uint64_roundtrip() {
        let original = Uint64(12_345_678_901_234_567);
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: Uint64 = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original.0, deserialized.0);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
