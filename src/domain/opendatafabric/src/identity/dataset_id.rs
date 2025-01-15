// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ed25519_dalek::SigningKey;

use super::{DatasetRef, DatasetRefAny, DatasetRefRemote};
use crate::formats::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Unique identifier of the dataset
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatasetID {
    did: DidOdf,
}

impl DatasetID {
    pub fn new(did: DidOdf) -> Self {
        Self { did }
    }

    /// Creates `DatasetID` from generated key pair using cryptographically
    /// secure RNG
    pub fn new_generated_ed25519() -> (SigningKey, Self) {
        let (key, did) = DidOdf::new_generated_ed25519();
        (key, Self::new(did))
    }

    /// For testing purposes only. Use [`DatasetID::new_generated_ed25519`] for
    /// cryptographically secure generation
    pub fn new_seeded_ed25519(seed: &[u8]) -> Self {
        Self::new(DidOdf::new_seeded_ed25519(seed))
    }

    pub fn as_did(&self) -> &DidOdf {
        &self.did
    }

    /// Reads `DatasetID` from canonical byte representation
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError<DidKey>> {
        Ok(Self::new(DidOdf::from_bytes(bytes)?))
    }

    /// Parses `DatasetID` from a canonical `did:odf:<multibase>` string
    pub fn from_did_str(s: &str) -> Result<Self, ParseError<DidOdf>> {
        Ok(Self::new(DidOdf::from_did_str(s)?))
    }

    /// Parses `DatasetID` from a multibase string (without `did:odf:`) prefix
    pub fn from_multibase_string(s: &str) -> Result<Self, ParseError<DidOdf>> {
        Ok(Self::new(DidOdf::from_multibase(s)?))
    }

    pub fn as_local_ref(&self) -> DatasetRef {
        DatasetRef::ID(self.clone())
    }

    pub fn into_local_ref(self) -> DatasetRef {
        DatasetRef::ID(self)
    }

    pub fn as_remote_ref(&self) -> DatasetRefRemote {
        DatasetRefRemote::from(self)
    }

    pub fn into_remote_ref(self) -> DatasetRefRemote {
        DatasetRefRemote::from(self)
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }

    pub fn into_any_ref(self) -> DatasetRefAny {
        DatasetRefAny::from(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Multiformat for DatasetID {
    fn format_name() -> &'static str {
        "did:odf"
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::ops::Deref for DatasetID {
    type Target = DidOdf;

    fn deref(&self) -> &Self::Target {
        &self.did
    }
}

impl From<DidOdf> for DatasetID {
    fn from(did: DidOdf) -> Self {
        Self::new(did)
    }
}

impl From<DidKey> for DatasetID {
    fn from(did: DidKey) -> Self {
        Self::new(DidOdf::from(did))
    }
}

impl From<DatasetID> for DidOdf {
    fn from(val: DatasetID) -> Self {
        val.did
    }
}

impl From<DatasetID> for DidKey {
    fn from(val: DatasetID) -> Self {
        val.did.into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for DatasetID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(&format!("DatasetID<{:?}>", Multicodec::Ed25519Pub))
            .field(&self.did.as_multibase())
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for DatasetID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_did_str())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Formats [`DatasetID`] as a canonical `did:odf:<multibase>` string
pub struct DatasetIDFmt<'a> {
    inner: DidKeyMultibaseFmt<'a>,
}

impl<'a> DatasetIDFmt<'a> {
    pub fn new(value: &'a DidKey) -> Self {
        Self {
            inner: DidKeyMultibaseFmt::new(value, Multibase::Base16),
        }
    }

    pub fn encoding(self, encoding: Multibase) -> Self {
        Self {
            inner: self.inner.encoding(encoding),
        }
    }
}

impl std::fmt::Debug for DatasetIDFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for DatasetIDFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "did:odf:{}", self.inner)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Serde
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for DatasetID {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.did.as_did_str())
    }
}

impl<'de> serde::Deserialize<'de> for DatasetID {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_string(DatasetIDSerdeVisitor)
    }
}

struct DatasetIDSerdeVisitor;

impl serde::de::Visitor<'_> for DatasetIDSerdeVisitor {
    type Value = DatasetID;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a DatasetID string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        DatasetID::from_did_str(v).map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
super::sqlx::impl_sqlx!(DatasetID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for DatasetID {}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for DatasetID {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([serde_json::json!(DatasetID::new_seeded_ed25519(b"dataset"))])
                .build(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
