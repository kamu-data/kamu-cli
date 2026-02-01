// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{Multibase, MultibaseError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Multibase-encoded private key
#[derive(Clone, PartialEq, Eq)]
pub struct PrivateKey(ed25519_dalek::SigningKey);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Don't leak secrets in debug logs
impl std::fmt::Debug for PrivateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrivateKey(***)")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl PrivateKey {
    pub fn from_bytes(bytes: &[u8; ed25519_dalek::SECRET_KEY_LENGTH]) -> Self {
        Self(ed25519_dalek::SigningKey::from_bytes(bytes))
    }

    pub fn from_multibase(s: &str) -> Result<Self, PrivateKeyDecodeError> {
        let mut buf = [0u8; ed25519_dalek::SECRET_KEY_LENGTH];
        let len = Multibase::decode(s, &mut buf)?;
        if len != buf.len() {
            Err(PrivateKeyDecodeError::InvalidLength {
                actual: len,
                expected: buf.len(),
            })?;
        }
        Ok(Self::from_bytes(&buf))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<ed25519_dalek::SigningKey> for PrivateKey {
    fn from(value: ed25519_dalek::SigningKey) -> Self {
        Self(value)
    }
}

impl From<PrivateKey> for ed25519_dalek::SigningKey {
    fn from(value: PrivateKey) -> Self {
        value.0
    }
}

impl std::ops::Deref for PrivateKey {
    type Target = ed25519_dalek::SigningKey;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for PrivateKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let multibase = Multibase::encode::<{ ed25519_dalek::SECRET_KEY_LENGTH * 2 }>(
            &self.0.to_bytes(),
            Multibase::Base64Url,
        );
        serializer.collect_str(&multibase)
    }
}

struct PrivateKeyVisitor;

impl serde::de::Visitor<'_> for PrivateKeyVisitor {
    type Value = PrivateKey;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a multibase-encoded private key")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        PrivateKey::from_multibase(v).map_err(serde::de::Error::custom)
    }
}

impl<'de> serde::Deserialize<'de> for PrivateKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(PrivateKeyVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum PrivateKeyDecodeError {
    #[error(transparent)]
    Multibase(#[from] MultibaseError),
    #[error("Invalid key length, expected {expected} actual {actual}")]
    InvalidLength { actual: usize, expected: usize },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "schemars")]
impl schemars::JsonSchema for PrivateKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "PrivateKey".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::PrivateKey").into()
    }

    fn json_schema(g: &mut schemars::SchemaGenerator) -> schemars::Schema {
        str::json_schema(g)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
