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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents DID in a custom `did:odf` method
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DidOdf {
    did: DidKey,
}

impl DidOdf {
    pub fn new(key_type: Multicodec, public_key: &[u8]) -> Result<Self, DidKeyError> {
        Ok(Self {
            did: DidKey::new(key_type, public_key)?,
        })
    }

    /// Creates DID from generated key pair using cryptographically secure RNG
    pub fn new_generated_ed25519() -> (ed25519_dalek::SigningKey, Self) {
        let (key, did) = DidKey::new_generated_ed25519();
        (key, Self::from(did))
    }

    /// For testing purposes only. Use [`DidKey::new_generated_ed25519`] for
    /// cryptographically secure generation
    pub fn new_seeded_ed25519(seed: &[u8]) -> Self {
        Self {
            did: DidKey::new_seeded_ed25519(seed),
        }
    }

    pub fn key_type(&self) -> Multicodec {
        self.did.key_type()
    }

    /// Map this ID into `did:key` method
    pub fn as_did_key(&self) -> &DidKey {
        &self.did
    }

    /// Returns an object representing canonical binary layout of this DID
    pub fn as_bytes(&self) -> DidKeyBytes {
        self.did.as_bytes()
    }

    /// Reads DID from canonical byte representation
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError<DidKey>> {
        Ok(Self::from(DidKey::from_bytes(bytes).map_err(|e| {
            DeserializeError::new_from(e.source.unwrap())
        })?))
    }

    /// Formats DID as a canonical `did:odf:<multibase>` string
    pub fn as_did_str(&self) -> DidOdfFmt {
        DidOdfFmt::new(&self.did)
    }

    /// Formats DID as a multibase string (without `did:odf:`) prefix
    pub fn as_multibase(&self) -> DidKeyMultibaseFmt {
        DidKeyMultibaseFmt::new(&self.did, Multibase::Base16)
    }

    /// Parses DID from a canonical `did:odf:<multibase>` string
    pub fn from_did_str(s: &str) -> Result<Self, ParseError<DidOdf>> {
        if !s.starts_with("did:odf:") {
            return Err(ParseError::new(s));
        }
        Self::from_multibase(&s[8..]).map_err(|e| ParseError::new_from(s, e))
    }

    /// Parses DID from a multibase string (without `did:odf:`) prefix
    pub fn from_multibase(s: &str) -> Result<Self, ParseError<DidOdf>> {
        Ok(Self::from(
            DidKey::from_multibase(s).map_err(|e| ParseError::new_from(s, e.source.unwrap()))?,
        ))
    }
}

impl Multiformat for DidOdf {
    fn format_name() -> &'static str {
        "did:odf"
    }
}

impl From<DidKey> for DidOdf {
    fn from(did: DidKey) -> Self {
        Self { did }
    }
}

impl From<DidOdf> for DidKey {
    fn from(val: DidOdf) -> Self {
        val.did
    }
}

impl std::fmt::Debug for DidOdf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(&format!("DidOdf<{:?}>", Multicodec::Ed25519Pub))
            .field(&self.as_multibase())
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Formats [`DidOdf`] as a canonical `did:odf:<multibase>` string
pub struct DidOdfFmt<'a> {
    inner: DidKeyMultibaseFmt<'a>,
}

impl<'a> DidOdfFmt<'a> {
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

impl std::fmt::Debug for DidOdfFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl std::fmt::Display for DidOdfFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "did:odf:{}", self.inner)
    }
}
