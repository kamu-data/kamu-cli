// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{Multibase, MultibaseError};
use crate::stack_string::StackString;

/// Multibase-encoded signature
#[derive(Debug, Clone)]
pub struct Signature(ed25519_dalek::Signature);

impl Signature {
    pub fn from_bytes(bytes: &[u8; ed25519_dalek::SIGNATURE_LENGTH]) -> Self {
        Self(ed25519_dalek::Signature::from_bytes(bytes))
    }

    pub fn from_multibase(s: &str) -> Result<Self, SignatureDecodeError> {
        let mut buf = [0u8; ed25519_dalek::SIGNATURE_LENGTH];
        let len = Multibase::decode(s, &mut buf)?;
        if len != buf.len() {
            Err(SignatureDecodeError::InvalidLength {
                actual: len,
                expected: buf.len(),
            })?;
        }
        Ok(Self::from_bytes(&buf))
    }

    pub fn to_multibase_str(&self) -> StackString<{ ed25519_dalek::SIGNATURE_LENGTH * 2 }> {
        Multibase::encode(&self.0.to_bytes(), Multibase::Base64Url)
    }
}

// Have to implement this as Deref exposes `to_string` from the wrapped type
// that produces incorrect value
impl std::fmt::Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_multibase_str())
    }
}

impl From<ed25519_dalek::Signature> for Signature {
    fn from(value: ed25519_dalek::Signature) -> Self {
        Self(value)
    }
}

impl From<Signature> for ed25519_dalek::Signature {
    fn from(value: Signature) -> Self {
        value.0
    }
}

impl std::ops::Deref for Signature {
    type Target = ed25519_dalek::Signature;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl serde::Serialize for Signature {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.to_multibase_str())
    }
}

struct SignatureVisitor;

impl serde::de::Visitor<'_> for SignatureVisitor {
    type Value = Signature;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a multibase-encoded signature")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Signature::from_multibase(v).map_err(serde::de::Error::custom)
    }
}

impl<'de> serde::Deserialize<'de> for Signature {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(SignatureVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SignatureDecodeError {
    #[error(transparent)]
    Multibase(#[from] MultibaseError),
    #[error("Invalid signature length, expected {expected} actual {actual}")]
    InvalidLength { actual: usize, expected: usize },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for Signature {}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for Signature {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([serde_json::json!(Signature::from_bytes(
                    &[0; ed25519_dalek::SIGNATURE_LENGTH]
                ))])
                .build(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
