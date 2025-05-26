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
pub enum AccountID {
    Odf(DidOdf),
}

impl AccountID {
    /// Creates `AccountID` from a generated key pair using cryptographically
    /// secure RNG
    pub fn new_generated_ed25519() -> (SigningKey, Self) {
        let (key, did) = DidOdf::new_generated_ed25519();
        (key, Self::Odf(did))
    }

    /// For testing purposes only. Use [`AccountID::new_generated_ed25519`] for
    /// cryptographically secure generation
    pub fn new_seeded_ed25519(seed: &[u8]) -> Self {
        Self::Odf(DidOdf::new_seeded_ed25519(seed))
    }

    pub fn as_did(&self) -> Option<&DidOdf> {
        match self {
            AccountID::Odf(did) => Some(did),
        }
    }

    /// Reads `AccountID` from canonical byte representation
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AccountIdParseBytesError> {
        if bytes.starts_with(DID_ODF_PREFIX.as_bytes()) {
            return Ok(Self::Odf(DidOdf::from_bytes(bytes)?));
        }

        if bytes.starts_with("did:pkh:".as_bytes()) {
            todo!("TODO: Wallet-based auth")
        }

        Err(AccountIdParseBytesError::InvalidValueFormat)
    }

    /// Parses `AccountID` from a canonical `did:odf:<multibase>`
    /// or `did:pkh:<address>` string
    pub fn from_did_str(s: &str) -> Result<Self, AccountIdParseStrError> {
        if let Some(stripped) = s.strip_prefix(DID_ODF_PREFIX) {
            return Self::from_multibase_string(stripped).map_err(Into::into);
        }

        if s.starts_with("did:pkh:") {
            todo!("TODO: Wallet-based auth")
        }

        Err(AccountIdParseStrError::InvalidValueFormat {
            value: s.to_string(),
        })
    }

    /// Parses `AccountID` from a multibase string (without `did:odf:`) prefix
    pub fn from_multibase_string(s: &str) -> Result<Self, ParseError<DidOdf>> {
        Ok(Self::Odf(DidOdf::from_multibase(s)?))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum AccountIdParseStrError {
    #[error(transparent)]
    OdfParseError(#[from] ParseError<DidOdf>),

    #[error("Invalid value format: {value}")]
    InvalidValueFormat { value: String },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum AccountIdParseBytesError {
    #[error(transparent)]
    OdfDeserializeError(#[from] DeserializeError<DidKey>),

    #[error("Invalid value format")]
    InvalidValueFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Multiformat for AccountID {
    fn format_name() -> &'static str {
        "did:odf"
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<DidOdf> for AccountID {
    fn from(did: DidOdf) -> Self {
        Self::Odf(did)
    }
}

impl From<DidKey> for AccountID {
    fn from(did: DidKey) -> Self {
        Self::Odf(DidOdf::from(did))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountID::Odf(did) => f
                .debug_tuple(&format!("AccountID<{:?}>", Multicodec::Ed25519Pub))
                .field(&did.as_multibase())
                .finish(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountID::Odf(did) => {
                write!(f, "{}", did.as_did_str())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Serde
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for AccountID {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self {
            AccountID::Odf(did) => serializer.collect_str(&did.as_did_str()),
        }
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
