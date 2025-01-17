// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ed25519_dalek::SigningKey;

use crate::formats::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Unique identifier of the account
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AccountID {
    did: DidOdf,
}

impl AccountID {
    pub fn new(did: DidOdf) -> Self {
        Self { did }
    }

    /// Creates `AccountID` from generated key pair using cryptographically
    /// secure RNG
    pub fn new_generated_ed25519() -> (SigningKey, Self) {
        let (key, did) = DidOdf::new_generated_ed25519();
        (key, Self::new(did))
    }

    /// For testing purposes only. Use [`AccountID::new_generated_ed25519`] for
    /// cryptographically secure generation
    pub fn new_seeded_ed25519(seed: &[u8]) -> Self {
        Self::new(DidOdf::new_seeded_ed25519(seed))
    }

    pub fn as_did(&self) -> &DidOdf {
        &self.did
    }

    /// Reads `AccountID` from canonical byte representation
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError<DidKey>> {
        Ok(Self::new(DidOdf::from_bytes(bytes)?))
    }

    /// Parses `AccountID` from a canonical `did:odf:<multibase>` string
    pub fn from_did_str(s: &str) -> Result<Self, ParseError<DidOdf>> {
        Ok(Self::new(DidOdf::from_did_str(s)?))
    }

    /// Parses `AccountID` from a multibase string (without `did:odf:`) prefix
    pub fn from_multibase_string(s: &str) -> Result<Self, ParseError<DidOdf>> {
        Ok(Self::new(DidOdf::from_multibase(s)?))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Multiformat for AccountID {
    fn format_name() -> &'static str {
        "did:odf"
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::ops::Deref for AccountID {
    type Target = DidOdf;

    fn deref(&self) -> &Self::Target {
        &self.did
    }
}

impl From<DidOdf> for AccountID {
    fn from(did: DidOdf) -> Self {
        Self::new(did)
    }
}

impl From<DidKey> for AccountID {
    fn from(did: DidKey) -> Self {
        Self::new(DidOdf::from(did))
    }
}

impl From<AccountID> for DidOdf {
    fn from(val: AccountID) -> Self {
        val.did
    }
}

impl From<AccountID> for DidKey {
    fn from(val: AccountID) -> Self {
        val.did.into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(&format!("AccountID<{:?}>", Multicodec::Ed25519Pub))
            .field(&self.did.as_multibase())
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_did_str())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Formats [`AccountID`] as a canonical `did:odf:<multibase>` string
pub struct AccountIDFmt<'a> {
    inner: DidKeyMultibaseFmt<'a>,
}

impl<'a> AccountIDFmt<'a> {
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

impl std::fmt::Debug for AccountIDFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for AccountIDFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "did:odf:{}", self.inner)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Serde
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for AccountID {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.did.as_did_str())
    }
}

impl<'de> serde::Deserialize<'de> for AccountID {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_string(AccountIDSerdeVisitor)
    }
}

struct AccountIDSerdeVisitor;

impl serde::de::Visitor<'_> for AccountIDSerdeVisitor {
    type Value = AccountID;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a AccountID string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        AccountID::from_did_str(v).map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
super::sqlx::impl_sqlx!(AccountID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for AccountID {}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for AccountID {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([serde_json::json!(AccountID::new_seeded_ed25519(b"account"))])
                .build(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
