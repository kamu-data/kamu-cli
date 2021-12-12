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

///////////////////////////////////////////////////////////////////////////////

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum MulticodecCode {
    Sha3_256 = 0x16,
    Arrow0_Sha3_256 = 0x300016,
    // When adding codes don't forget to add them into TryFrom<u32> below
}

// TODO: use num-derive
impl TryFrom<u32> for MulticodecCode {
    type Error = MulticodecError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0x16 => Ok(MulticodecCode::Sha3_256),
            0x300016 => Ok(MulticodecCode::Arrow0_Sha3_256),
            _ => Err(MulticodecError::UnsupportedCode(value)),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

pub type Multihash = MultihashGeneric<32>;
pub type MultihashShort<'a> = MultihashShortGeneric<'a, 32>;

///////////////////////////////////////////////////////////////////////////////

/// S specifies maximum hash size in bytes(!) this type can handle, not the exact length of it
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct MultihashGeneric<const S: usize> {
    /// The code of the Multihash.
    code: MulticodecCode,
    /// The actual length of the digest in bytes (not the allocated size).
    len: usize,
    /// The digest.
    buf: [u8; S],
}

impl<const S: usize> MultihashGeneric<S> {
    pub fn new(code: MulticodecCode, digest: &[u8]) -> Self {
        assert!(digest.len() <= S);

        let mut inst = Self {
            code,
            len: digest.len(),
            buf: [0; S],
        };

        inst.buf[..digest.len()].copy_from_slice(digest);
        inst
    }

    pub fn from_digest<D: digest::Digest>(code: MulticodecCode, data: &[u8]) -> Self {
        assert!(D::output_size() <= S);
        let digest = D::digest(data);
        Self::new(code, &digest)
    }

    pub fn digest(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MultihashError> {
        let (code, rest) = uvar::decode::u32(bytes)?;
        let code: MulticodecCode = code.try_into()?;
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
        assert_eq!(s.as_bytes()[0], b'z');
        let buf = bs58::decode(&s.split_at(1).1).into_vec()?;
        Self::from_bytes(&buf)
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

// This is the trait that informs Serde how to deserialize MyMap.
impl<'de> Deserialize<'de> for Multihash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(MultihashVisitor)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum MulticodecError {
    #[error("Unsupported code: {0}")]
    UnsupportedCode(u32),
}

#[derive(Error, Debug)]
pub enum MultihashError {
    #[error(transparent)]
    Multicodec(#[from] MulticodecError),
    #[error("Digest of {0} bytes is too long")]
    TooLong(usize),
    #[error("Malformed data")]
    Malformed,
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
        let digest = self.0.digest();
        let tail_len = std::cmp::min(self.1 / 2, digest.len()); // 1 byte == 2 base16 characters
        let s = hex::encode(&digest[digest.len() - tail_len..]);
        write!(f, "{}", s)
    }
}
