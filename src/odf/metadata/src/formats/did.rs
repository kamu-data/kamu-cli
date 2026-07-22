// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use multiformats::stack_string::StackString;
use multiformats::*;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Revise with `multidid` spec in mind
/// Represents a DID in one of the known methods
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Did {
    Key(DidKey),
    Odf(DidOdf),
    Pkh(DidPkh),
}

impl Did {
    /// Returns an object representing canonical binary layout of this DID
    pub fn as_bytes(&self) -> &[u8] {
        todo!("We don't have canonical byte layouts for all DIDs yet")
    }

    /// Reads DID from canonical byte representation
    pub fn from_bytes(_bytes: &[u8]) -> Result<Self, DeserializeError<Did>> {
        todo!("We don't have canonical byte layouts for all DIDs yet")
    }

    /// Formats DID as a canonical `did:<method>:<data>` string
    pub fn as_did_str(&self) -> DidFmt<'_> {
        DidFmt::new(self)
    }

    /// Parses DID from a canonical `did:odf:<multibase>` string
    pub fn from_did_str(s: &str) -> Result<Self, ParseError<DidOdf>> {
        if let Some(stripped) = s.strip_prefix(DID_ODF_PREFIX) {
            DidOdf::from_multibase(stripped)
                .map(Self::Odf)
                .map_err(ParseError::convert)
        } else if let Some(stripped) = s.strip_prefix(DID_KEY_PREFIX) {
            DidKey::from_multibase(stripped)
                .map(Self::Key)
                .map_err(ParseError::convert)
        } else if let Some(stripped) = s.strip_prefix(DID_PKH_PREFIX) {
            DidPkh::parse_caip10_account_id(stripped)
                .map(Self::Pkh)
                .map_err(|_| ParseError::new(s))
        } else {
            Err(ParseError::new(s))
        }
    }
}

impl Multiformat for Did {
    fn format_name() -> &'static str {
        "did:odf"
    }
}

impl From<DidKey> for Did {
    fn from(val: DidKey) -> Self {
        Self::Key(val)
    }
}

impl From<DidOdf> for Did {
    fn from(val: DidOdf) -> Self {
        Self::Odf(val)
    }
}

impl From<DidPkh> for Did {
    fn from(val: DidPkh) -> Self {
        Self::Pkh(val)
    }
}

impl std::fmt::Debug for Did {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_did_str())
    }
}

impl std::fmt::Display for Did {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_did_str())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Serde
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for Did {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.as_did_str())
    }
}

impl<'de> serde::Deserialize<'de> for Did {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = Did;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a canonical `did:odf:<multibase>` string")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Did::from_did_str(v).map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_string(Visitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for Did {}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for Did {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([serde_json::json!(Self::Odf(DidOdf::new_seeded_ed25519(
                    b"sample"
                )))])
                .build(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Formats [`Did`] as a canonical `did:<method>:<data>` string
pub struct DidFmt<'a> {
    did: &'a Did,
}

impl<'a> DidFmt<'a> {
    pub fn new(did: &'a Did) -> Self {
        Self { did }
    }

    pub fn to_stack_string(self) -> StackString<MAX_DID_CANONICAL_STRING_REPR_LEN> {
        use std::io::Write;
        let mut buf = [0u8; MAX_DID_CANONICAL_STRING_REPR_LEN];

        let len = {
            let mut c = std::io::Cursor::new(&mut buf[..]);
            write!(c, "{self}").unwrap();
            usize::try_from(c.position()).unwrap()
        };

        StackString::new(buf, len)
    }
}

impl std::fmt::Debug for DidFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for DidFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.did {
            Did::Key(did) => write!(f, "{}", did.as_did_str()),
            Did::Odf(did) => write!(f, "{}", did.as_did_str()),
            Did::Pkh(did) => write!(f, "{}", did.as_did_str()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
