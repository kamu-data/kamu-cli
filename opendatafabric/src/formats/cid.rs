// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::{Serialize, Serializer};
use thiserror::Error;
use unsigned_varint as uvar;

use super::*;

///////////////////////////////////////////////////////////////////////////////

pub type CID = CIDGeneric<{ super::multihash::MAX_HASH_LENGTH_BYTES }>;

///////////////////////////////////////////////////////////////////////////////

/// CIDv1
/// See https://github.com/multiformats/cid
/// S specifies maximum hash size in bytes(!) this type can handle, not the exact length of it
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CIDGeneric<const S: usize> {
    /// Content type or format of the data being addressed
    content_type: Multicodec,
    /// Multihash of the content being addressed
    content_address: MultihashGeneric<S>,
}

impl<const S: usize> CIDGeneric<S> {
    pub fn new(content_type: Multicodec, content_address: MultihashGeneric<S>) -> Self {
        Self {
            content_type,
            content_address,
        }
    }

    #[inline]
    pub fn content_type(&self) -> Multicodec {
        self.content_type
    }

    #[inline]
    pub fn content_address(&self) -> &MultihashGeneric<S> {
        &self.content_address
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CIDError> {
        let (cid_version, bytes) = uvar::decode::u32(bytes)?;
        if cid_version != (Multicodec::CIDv1 as u32) {
            return Err(CIDError::UnsupportedVersion(cid_version));
        }

        let (content_type, bytes) = uvar::decode::u32(bytes)?;
        let content_type: Multicodec = content_type.try_into()?;

        let content_address = MultihashGeneric::<S>::from_bytes(bytes)?;

        Ok(Self::new(content_type, content_address))
    }

    // TODO: PERF: This is inefficient
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut varint_buf = uvar::encode::u32_buffer();

        let varint = uvar::encode::u32(Multicodec::CIDv1 as u32, &mut varint_buf);
        buf.extend_from_slice(varint);

        let varint = uvar::encode::u32(self.content_type as u32, &mut varint_buf);
        buf.extend_from_slice(varint);

        buf.extend(self.content_address.to_bytes());
        buf
    }

    // TODO: PERF: This is inefficient
    pub fn from_multibase_str(s: &str) -> Result<Self, CIDError> {
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
}

///////////////////////////////////////////////////////////////////////////////

impl<const S: usize> fmt::Debug for CIDGeneric<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple(&format!("CID<{:?}>", self.content_type))
            .field(&self.to_multibase_string())
            .finish()
    }
}

impl<const S: usize> fmt::Display for CIDGeneric<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.to_multibase_string();
        write!(f, "{}", s)
    }
}

impl<const S: usize> Serialize for CIDGeneric<S> {
    fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.collect_str(self)
    }
}

struct CIDVisitor;

impl<'de> Visitor<'de> for CIDVisitor {
    type Value = CID;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a multibase-encoded CID")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        CID::from_multibase_str(v).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for CID {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(CIDVisitor)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CIDError {
    #[error("Unsupported CID version: {0}")]
    UnsupportedVersion(u32),
    #[error(transparent)]
    Multicodec(#[from] MulticodecError),
    #[error(transparent)]
    Multihash(#[from] MultihashError),
    #[error("Malformed data")]
    Malformed,
}

impl From<bs58::decode::Error> for CIDError {
    fn from(_: bs58::decode::Error) -> Self {
        Self::Malformed
    }
}

impl From<uvar::decode::Error> for CIDError {
    fn from(_: uvar::decode::Error) -> Self {
        Self::Malformed
    }
}
