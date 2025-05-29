// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ed25519_dalek::SigningKey;
use multiformats::stack_string::AsStackString;

use crate::formats::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MAX_ACCOUNT_ID_STRING_REPR_LEN: usize = {
    let a = MAX_DID_CANONICAL_STRING_REPR_LEN;
    let b = MAX_DID_PKH_STRING_REPR_LEN;

    if a > b {
        a
    } else {
        b
    }
};
pub const MAX_ACCOUNT_ID_STRING_WITHOUT_DID_PREFIX_REPR_LEN: usize = {
    let a = MAX_DID_CANONICAL_STRING_REPR_LEN
        .checked_sub(DID_ODF_PREFIX.len())
        .unwrap();
    let b = CAIP_10_ACCOUNT_ADDRESS_MAX_LENGTH
        .checked_sub(DID_PKH_PREFIX.len())
        .unwrap();

    if a > b {
        a
    } else {
        b
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Unique identifier of the account
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AccountID {
    Odf(DidOdf),
    Pkh(DidPkh),
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

    pub fn as_did_odf(&self) -> Option<&DidOdf> {
        match self {
            Self::Odf(did) => Some(did),
            Self::Pkh(_) => None,
        }
    }

    pub fn as_did_pkh(&self) -> Option<&DidPkh> {
        match self {
            Self::Odf(_) => None,
            Self::Pkh(pkh) => Some(pkh),
        }
    }

    /// Reads `AccountID` from canonical byte representation
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AccountIdParseBytesError> {
        if bytes.starts_with(DID_ODF_PREFIX.as_bytes()) {
            return Ok(Self::Odf(DidOdf::from_bytes(bytes)?));
        }

        if bytes.starts_with(DID_PKH_PREFIX.as_bytes()) {
            unimplemented!("Use AccountID::from_did_str() for {DID_PKH_PREFIX}")
        }

        Err(AccountIdParseBytesError::InvalidValueFormat)
    }

    /// Parses `AccountID` from a canonical `did:odf:<multibase>`
    /// or `did:pkh:<account_id(CAIP-10)>` string
    pub fn from_did_str(s: &str) -> Result<Self, AccountIdParseStrError> {
        if let Some(stripped) = s.strip_prefix(DID_ODF_PREFIX) {
            return Self::from_multibase_string(stripped).map_err(Into::into);
        }

        if let Some(stripped) = s.strip_prefix(DID_PKH_PREFIX) {
            return Self::parse_caip10_account_id(stripped).map_err(Into::into);
        }

        Err(AccountIdParseStrError::InvalidValueFormat {
            value: s.to_string(),
        })
    }

    pub fn parse_id_without_did_prefix(s: &str) -> Result<Self, AccountIdParseStrError> {
        if let Ok(id) = Self::from_multibase_string(s) {
            return Ok(id);
        }

        if let Ok(id) = Self::parse_caip10_account_id(s) {
            return Ok(id);
        }

        Err(AccountIdParseStrError::InvalidValueFormat {
            value: s.to_string(),
        })
    }

    /// Parses `AccountID` from a multibase string (without `did:odf:`) prefix
    pub fn from_multibase_string(s: &str) -> Result<Self, ParseError<DidOdf>> {
        Ok(Self::Odf(DidOdf::from_multibase(s)?))
    }

    /// Parses `AccountID` from a CAIP-10 account ID string (without `did:pkh:`)
    /// prefix
    pub fn parse_caip10_account_id(s: &str) -> Result<Self, DidPkhParseError> {
        Ok(Self::Pkh(DidPkh::parse_caip10_account_id(s)?))
    }

    pub fn as_id_without_did_prefix(&self) -> IdWithoutDidPrefixFmt {
        IdWithoutDidPrefixFmt { value: self }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct IdWithoutDidPrefixFmt<'a> {
    value: &'a AccountID,
}

impl std::fmt::Display for IdWithoutDidPrefixFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value {
            AccountID::Odf(odf) => write!(f, "{}", odf.as_multibase()),
            AccountID::Pkh(pkh) => {
                let wallet = pkh.wallet_address();

                // NOTE: We explicitly use only the address part (w/o chain id (CAIP-2))
                //       since the wallet address is already unique.
                write!(f, "{}", wallet.strip_prefix("0x").unwrap_or(wallet))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AsStackString<MAX_ACCOUNT_ID_STRING_REPR_LEN> for AccountID {}
impl AsStackString<MAX_ACCOUNT_ID_STRING_REPR_LEN> for &AccountID {}

impl AsStackString<MAX_ACCOUNT_ID_STRING_WITHOUT_DID_PREFIX_REPR_LEN>
    for IdWithoutDidPrefixFmt<'_>
{
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum AccountIdParseStrError {
    #[error(transparent)]
    OdfParseError(#[from] ParseError<DidOdf>),

    #[error(transparent)]
    PkhParseError(#[from] DidPkhParseError),

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
    fn from(odf: DidKey) -> Self {
        Self::Odf(DidOdf::from(odf))
    }
}

impl From<DidPkh> for AccountID {
    fn from(pkh: DidPkh) -> Self {
        Self::Pkh(pkh)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Odf(odf) => f
                .debug_tuple(&format!("AccountID<{:?}>", Multicodec::Ed25519Pub))
                .field(&odf.as_multibase())
                .finish(),
            Self::Pkh(pkh) => f.debug_tuple("AccountID").field(&pkh).finish(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Odf(odf) => {
                write!(f, "{}", odf.as_did_str())
            }
            Self::Pkh(pkh) => {
                write!(f, "{}", pkh.as_did_str())
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
            Self::Odf(odf) => serializer.collect_str(&odf.as_did_str()),
            Self::Pkh(pkh) => serializer.collect_str(&pkh.as_did_str()),
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
        use serde_json::json;
        use utoipa::openapi::schema::*;

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([
                    json!(AccountID::new_seeded_ed25519(b"account")),
                    json!(AccountID::parse_caip10_account_id(
                        "eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a"
                    )
                    .unwrap()),
                ])
                .build(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
