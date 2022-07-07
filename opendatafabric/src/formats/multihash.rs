// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::{Serialize, Serializer};
use std::fmt;
use thiserror::Error;
use unsigned_varint as uvar;

use crate::{Multicodec, MulticodecError, InvalidValue};

///////////////////////////////////////////////////////////////////////////////

pub const MAX_HASH_LENGTH_BYTES: usize = 32;

pub type Multihash = MultihashGeneric<MAX_HASH_LENGTH_BYTES>;
pub type MultihashShort<'a> = MultihashShortGeneric<'a, MAX_HASH_LENGTH_BYTES>;

///////////////////////////////////////////////////////////////////////////////

/// S specifies maximum hash size in bytes(!) this type can handle, not the exact length of it
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MultihashGeneric<const S: usize> {
    /// The code of the Multihash.
    code: Multicodec,
    /// The actual length of the digest in bytes (not the allocated size).
    len: usize,
    /// The digest.
    buf: [u8; S],
}

impl<const S: usize> MultihashGeneric<S> {
    pub fn new(code: Multicodec, digest: &[u8]) -> Self {
        assert!(
            digest.len() <= S,
            "Digest length is greater than max length"
        );

        let mut inst = Self {
            code,
            len: digest.len(),
            buf: [0; S],
        };

        inst.buf[..digest.len()].copy_from_slice(digest);
        inst
    }

    pub fn from_digest<D: digest::Digest>(code: Multicodec, data: &[u8]) -> Self {
        assert!(
            <D as digest::Digest>::output_size() <= S,
            "Digest length is greater than max length"
        );
        let digest = D::digest(data);
        Self::new(code, &digest)
    }

    pub fn from_digest_sha3_256(data: &[u8]) -> Self {
        Self::from_digest::<sha3::Sha3_256>(Multicodec::Sha3_256, data)
    }

    pub fn digest(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MultihashError> {
        let (code, rest) = uvar::decode::u32(bytes)?;
        let code: Multicodec = code.try_into()?;
        let (len, rest) = uvar::decode::usize(rest)?;
        if len > S {
            Err(MultihashError::TooLong(len))
        } else if len != rest.len() {
            Err(MultihashError::Malformed)
        } else {
            Ok(Self::new(code, rest))
        }
    }

    // TODO: Avoid allocations in serde
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut varint_buf = uvar::encode::u32_buffer();
        let varint = uvar::encode::u32(self.code as u32, &mut varint_buf);
        buf.extend_from_slice(varint);
        let varint = uvar::encode::u32(self.len as u32, &mut varint_buf);
        buf.extend_from_slice(varint);
        buf.extend(self.digest());
        buf
    }

    // TODO: PERF: This is inefficient
    pub fn from_multibase_str(s: &str) -> Result<Self, MultihashError> {
        if s.as_bytes()[0] == b'z' {
            let buf = bs58::decode(&s.split_at(1).1).into_vec()?;
            Self::from_bytes(&buf)
        } else {
            Err(MultihashError::NoMultibytePrefix)
        }
    }

    // TODO: PERF: This is inefficient
    pub fn to_multibase_string(&self) -> String {
        let bin = self.to_bytes();
        let s = bs58::encode(&bin).into_string();
        format!("z{}", s)
    }

    pub fn short(&self) -> MultihashShortGeneric<S> {
        MultihashShortGeneric::new(self)
    }
}

///////////////////////////////////////////////////////////////////////////////

impl TryFrom<&str> for Multihash {
    type Error = InvalidValue<Multihash>;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
         match Multihash::from_multibase_str(s) {
            Ok(v) => Ok(v),
            Err(_) => Err(InvalidValue::new(s)),
         }
    }
}

crate::formats::impl_invalid_value!(Multihash);

///////////////////////////////////////////////////////////////////////////////

impl<const S: usize> fmt::Debug for MultihashGeneric<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("Multihash<{:?}>", self.code))
            .field(&self.to_multibase_string())
            .finish()
    }
}

impl<const S: usize> fmt::Display for MultihashGeneric<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.to_multibase_string();
        write!(f, "{}", s)
    }
}

impl<const S: usize> Serialize for MultihashGeneric<S> {
    fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.collect_str(self)
    }
}

struct MultihashVisitor;

impl<'de> Visitor<'de> for MultihashVisitor {
    type Value = Multihash;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a multibase-encoded multihash")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        Multihash::from_multibase_str(v).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Multihash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(MultihashVisitor)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum MultihashError {
    #[error(transparent)]
    Multicodec(#[from] MulticodecError),
    #[error("Digest of {0} bytes is too long")]
    TooLong(usize),
    #[error("Malformed data")]
    Malformed,
    #[error("Should have multibyte 'z' prefix")]
    NoMultibytePrefix,
}

impl From<bs58::decode::Error> for MultihashError {
    fn from(_: bs58::decode::Error) -> Self {
        Self::Malformed
    }
}

impl From<uvar::decode::Error> for MultihashError {
    fn from(_: uvar::decode::Error) -> Self {
        Self::Malformed
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy)]
pub struct MultihashShortGeneric<'a, const S: usize>(&'a MultihashGeneric<S>, usize);

impl<'a, const S: usize> MultihashShortGeneric<'a, S> {
    const DEFAULT_LENGTH: usize = 8;

    pub fn new(hash: &'a MultihashGeneric<S>) -> Self {
        Self::new_with_length(hash, Self::DEFAULT_LENGTH)
    }

    pub fn new_with_length(hash: &'a MultihashGeneric<S>, len: usize) -> Self {
        assert_eq!(len & 1, 0);
        Self(hash, len)
    }
}

impl<const S: usize> fmt::Display for MultihashShortGeneric<'_, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.0.to_multibase_string();
        let start = s.len() - std::cmp::min(self.1, s.len());
        write!(f, "{}", &s[start..])
    }
}
