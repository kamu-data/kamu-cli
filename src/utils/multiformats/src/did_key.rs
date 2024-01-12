// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use {ed25519_dalek as ed25519, unsigned_varint as uvar};

use super::*;
use crate::stack_string::StackString;

////////////////////////////////////////////////////////////////////////////////

// TODO: Rethink after stabilization of `generic_const_exprs`
pub const MAX_DID_KEY_LEN: usize = ed25519::PUBLIC_KEY_LENGTH;
pub const MAX_DID_BINARY_REPR_LEN: usize = MAX_VARINT_LEN + MAX_DID_KEY_LEN;
pub const MAX_DID_MULTIBASE_REPR_LEN: usize = 1 + MAX_DID_BINARY_REPR_LEN * 2; // Assuming base16 worst case encoding
pub const MAX_DID_CANONICAL_STRING_REPR_LEN: usize = "did:key:".len() + MAX_DID_MULTIBASE_REPR_LEN;
pub const DEFAULT_DID_MULTIBASE_ENCODING: Multibase = Multibase::Base58Btc;

////////////////////////////////////////////////////////////////////////////////

/// Decentralized identifier that follows W3C [`did:key` method](https://w3c-ccg.github.io/did-method-key/)
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DidKey {
    public_key: [u8; MAX_DID_KEY_LEN],
}

impl DidKey {
    pub fn new(key_type: Multicodec, public_key: &[u8]) -> Result<Self, DidKeyError> {
        if key_type != Multicodec::Ed25519Pub {
            return Err(DidKeyError::UnsupportedKeyType(key_type));
        }
        assert_eq!(
            public_key.len(),
            ed25519::PUBLIC_KEY_LENGTH,
            "Ed25519Pub key expects {} bytes but got {}",
            ed25519::PUBLIC_KEY_LENGTH,
            public_key.len()
        );

        let mut pk = [0 as u8; MAX_DID_KEY_LEN];
        pk.clone_from_slice(public_key);

        Ok(Self { public_key: pk })
    }

    /// Creates DID from generated key pair using cryptographically secure RNG
    pub fn new_generated_ed25519() -> (ed25519::SigningKey, Self) {
        use rand::rngs::OsRng;

        let mut csprng = OsRng {};
        let keypair = ed25519::SigningKey::generate(&mut csprng);
        let pub_key = keypair.verifying_key().to_bytes();
        let id = Self::new(Multicodec::Ed25519Pub, &pub_key).unwrap();
        (keypair, id)
    }

    /// For testing purposes only. Use [DidKey::new_generated_ed25519] for
    /// cryptographically secure generation
    pub fn new_seeded_ed25519(seed: &[u8]) -> Self {
        use rand::rngs::SmallRng;
        use rand::{RngCore, SeedableRng};

        let mut seed_buf = [0 as u8; 32];
        seed_buf[..seed.len()].copy_from_slice(seed);
        let mut prng = SmallRng::from_seed(seed_buf);

        let mut public_key = [0 as u8; ed25519_dalek::PUBLIC_KEY_LENGTH];
        prng.fill_bytes(&mut public_key[..]);

        Self::new(Multicodec::Ed25519Pub, &public_key).unwrap()
    }

    pub fn key_type(&self) -> Multicodec {
        Multicodec::Ed25519Pub
    }

    /// Returns an object representing canonical binary layout of this DID
    pub fn as_bytes(&self) -> DidKeyBytes {
        DidKeyBytes::new(self)
    }

    /// Formats DID as a canonical `did:key:<multibase>` string
    pub fn as_did_str(&self) -> DidKeyFmt {
        DidKeyFmt::new(self, DEFAULT_DID_MULTIBASE_ENCODING)
    }

    /// Formats DID as a multibase string (without `did:key:`) prefix
    pub fn as_multibase(&self) -> DidKeyMultibaseFmt {
        DidKeyMultibaseFmt::new(self, DEFAULT_DID_MULTIBASE_ENCODING)
    }

    /// Reads DID from canonical byte representation
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError<DidKey>> {
        let (key_type, key_bytes) =
            uvar::decode::u32(bytes).map_err(|e| DeserializeError::new_from(e))?;

        let key_type: Multicodec = key_type
            .try_into()
            .map_err(|e| DeserializeError::new_from(e))?;

        Self::new(key_type, key_bytes).map_err(|e| DeserializeError::new_from(e))
    }

    /// Parses DID from a canonical `did:key:<multibase>` string
    pub fn from_did_str(s: &str) -> Result<Self, ParseError<DidKey>> {
        if !s.starts_with("did:key:") {
            return Err(ParseError::new(s));
        }
        Self::from_multibase(&s[8..]).map_err(|e| ParseError::new_from(s, e))
    }

    /// Parses DID from a multibase string (without `did:key:`) prefix
    pub fn from_multibase(s: &str) -> Result<Self, ParseError<DidKey>> {
        let mut buf = [0 as u8; MAX_VARINT_LEN + ed25519_dalek::PUBLIC_KEY_LENGTH];
        let len = Multibase::decode(s, &mut buf[..]).map_err(|e| ParseError::new_from(s, e))?;
        Self::from_bytes(&buf[..len]).map_err(|e| ParseError::new_from(s, e))
    }
}

impl Multiformat for DidKey {
    fn format_name() -> &'static str {
        "did:key"
    }
}

impl std::fmt::Debug for DidKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(&format!("DidKey<{:?}>", Multicodec::Ed25519Pub))
            .field(&self.as_multibase())
            .finish()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DidKeyError {
    #[error("Unsupported key type '{0}' key must be 'ed25519-pub")]
    UnsupportedKeyType(Multicodec),
}

////////////////////////////////////////////////////////////////////////////////

/// Represents [DidKey] in a canonical binary layout
pub struct DidKeyBytes {
    buf: [u8; MAX_DID_BINARY_REPR_LEN],
    len: usize,
}

impl DidKeyBytes {
    fn new(value: &DidKey) -> Self {
        use std::io::Write;
        let mut buf = [0 as u8; MAX_DID_BINARY_REPR_LEN];

        let len = {
            let mut cursor = std::io::Cursor::new(&mut buf[..]);

            let mut varint_buf = uvar::encode::u32_buffer();
            let varint = uvar::encode::u32(Multicodec::Ed25519Pub as u32, &mut varint_buf);

            cursor.write_all(varint).unwrap();
            cursor.write_all(&value.public_key).unwrap();
            cursor.position() as usize
        };

        Self { buf, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    pub fn write(&self, mut w: impl std::io::Write) -> Result<(), std::io::Error> {
        w.write_all(self.as_slice())
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
}

impl AsRef<[u8]> for DidKeyBytes {
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Formats [DidKey] as a canonical `did:key:<multibase>` string
pub struct DidKeyFmt<'a> {
    inner: DidKeyMultibaseFmt<'a>,
}

impl<'a> DidKeyFmt<'a> {
    pub fn new(value: &'a DidKey, encoding: Multibase) -> Self {
        Self {
            inner: DidKeyMultibaseFmt::new(value, encoding),
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
            write!(c, "{}", self).unwrap();
            c.position() as usize
        };

        StackString::new(buf, len)
    }
}

impl<'a> std::fmt::Debug for DidKeyFmt<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<'a> std::fmt::Display for DidKeyFmt<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "did:key:{}", self.inner)
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Formats [DidKey] as a multibase string (without `did:key:`) prefix
pub struct DidKeyMultibaseFmt<'a> {
    value: &'a DidKey,
    encoding: Multibase,
}

impl<'a> DidKeyMultibaseFmt<'a> {
    pub fn new(value: &'a DidKey, encoding: Multibase) -> Self {
        Self { value, encoding }
    }

    pub fn encoding(self, encoding: Multibase) -> Self {
        Self { encoding, ..self }
    }

    pub fn to_stack_string(self) -> StackString<MAX_DID_MULTIBASE_REPR_LEN> {
        Multibase::encode::<MAX_DID_MULTIBASE_REPR_LEN>(
            &self.value.as_bytes().as_slice(),
            self.encoding,
        )
    }
}

impl<'a> std::fmt::Debug for DidKeyMultibaseFmt<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<'a> std::fmt::Display for DidKeyMultibaseFmt<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            Multibase::format::<MAX_DID_MULTIBASE_REPR_LEN>(
                &self.value.as_bytes().as_slice(),
                self.encoding
            )
        )
    }
}

////////////////////////////////////////////////////////////////////////////////
