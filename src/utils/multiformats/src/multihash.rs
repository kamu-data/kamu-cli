// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::{Serialize, Serializer};
use unsigned_varint as uvar;

use super::*;
use crate::stack_string::StackString;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Rethink after stabilization of `generic_const_exprs`
pub const MAX_MULTIHASH_DIGEST_LEN: usize = 32;
pub const MAX_MULTIHASH_BINARY_REPR_LEN: usize =
    MAX_VARINT_LEN + MAX_VARINT_LEN + MAX_MULTIHASH_DIGEST_LEN;
pub const MAX_MULTIHASH_MULTIBASE_REPR_LEN: usize = 1 + MAX_MULTIHASH_BINARY_REPR_LEN * 2; // Assuming base16 worst case encoding
pub const DEFAULT_MULTIHASH_MULTIBASE_ENCODING: Multibase = Multibase::Base16;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// S specifies maximum hash size in bytes(!) this type can handle, not the
/// exact length of it
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Multihash {
    /// The digest buffer (maybe longer than the digest)
    buf: [u8; MAX_MULTIHASH_DIGEST_LEN],
    /// The actual length of the digest in bytes
    len: usize,
    /// The code of the Multihash
    code: Multicodec,
}

impl Multihash {
    pub fn new(code: Multicodec, digest: &[u8]) -> Result<Self, BufferTooSmall> {
        if digest.len() > MAX_MULTIHASH_DIGEST_LEN {
            return Err(BufferTooSmall);
        }

        let mut buf = [0; MAX_MULTIHASH_DIGEST_LEN];
        buf[..digest.len()].copy_from_slice(digest);

        Ok(Self {
            buf,
            len: digest.len(),
            code,
        })
    }

    pub fn from_digest<D: digest::Digest>(code: Multicodec, data: &[u8]) -> Self {
        // TODO: Replace with static assertion
        assert!(
            <D as digest::Digest>::output_size() <= MAX_MULTIHASH_DIGEST_LEN,
            "Buffer too small"
        );
        let digest = D::digest(data);
        Self::new(code, &digest).unwrap()
    }

    pub fn from_digest_sha3_256(data: &[u8]) -> Self {
        Self::from_digest::<sha3::Sha3_256>(Multicodec::Sha3_256, data)
    }

    pub fn from_multibase(s: &str) -> Result<Self, ParseError<Multihash>> {
        let mut buf = [0_u8; MAX_MULTIHASH_BINARY_REPR_LEN];
        let len = Multibase::decode(s, &mut buf[..]).map_err(|e| ParseError::new_from(s, e))?;
        Self::from_bytes(&buf[..len]).map_err(|e| ParseError::new_from(s, e))
    }

    pub fn as_multibase(&self) -> MultihashFmt<'_> {
        MultihashFmt::new(self, DEFAULT_MULTIHASH_MULTIBASE_ENCODING)
    }

    pub fn digest(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    pub fn code(&self) -> Multicodec {
        self.code
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError<Multihash>> {
        let (code, rest) = uvar::decode::u32(bytes).map_err(DeserializeError::new_from)?;
        let code: Multicodec = code.try_into().map_err(DeserializeError::new_from)?;

        let (len, rest) = uvar::decode::usize(rest).map_err(DeserializeError::new_from)?;
        if len != rest.len() {
            Err(DeserializeError::new())
        } else {
            Self::new(code, rest).map_err(DeserializeError::new_from)
        }
    }

    pub fn as_bytes(&self) -> MultihashBytes {
        MultihashBytes::new(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Multiformat for Multihash {
    fn format_name() -> &'static str {
        "multihash"
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for Multihash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(&format!("Multihash<{:?}>", self.code))
            .field(&self.as_multibase())
            .finish()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Display using the default multibase encoding
impl std::fmt::Display for Multihash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_multibase())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Serialize for Multihash {
    fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        serializer.collect_str(&self.as_multibase())
    }
}

struct MultihashVisitor;

impl Visitor<'_> for MultihashVisitor {
    type Value = Multihash;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a multibase-encoded multihash")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        Multihash::from_multibase(v).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Multihash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(MultihashVisitor)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents [Multihash] in a canonical binary layout
pub struct MultihashBytes {
    buf: [u8; MAX_MULTIHASH_BINARY_REPR_LEN],
    len: usize,
}

impl MultihashBytes {
    fn new(value: &Multihash) -> Self {
        use std::io::Write;

        let mut buf = [0_u8; MAX_MULTIHASH_BINARY_REPR_LEN];

        let len = {
            let mut cursor = std::io::Cursor::new(&mut buf[..]);

            let mut varint_buf = uvar::encode::u32_buffer();
            let varint = uvar::encode::u32(value.code as u32, &mut varint_buf);
            cursor.write_all(varint).unwrap();

            let varint = uvar::encode::u32(u32::try_from(value.len).unwrap(), &mut varint_buf);
            cursor.write_all(varint).unwrap();

            cursor.write_all(value.digest()).unwrap();

            usize::try_from(cursor.position()).unwrap()
        };

        Self { buf, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

impl AsRef<[u8]> for MultihashBytes {
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MultihashFmt<'a> {
    value: &'a Multihash,
    encoding: Multibase,
    short_len: Option<usize>,
}

impl<'a> MultihashFmt<'a> {
    const DEFAULT_SHORT_LENGTH: usize = 8;

    fn new(value: &'a Multihash, encoding: Multibase) -> Self {
        Self {
            value,
            encoding,
            short_len: None,
        }
    }

    pub fn encoding(self, encoding: Multibase) -> Self {
        Self { encoding, ..self }
    }

    pub fn short(self) -> Self {
        self.short_of_length(Self::DEFAULT_SHORT_LENGTH)
    }

    pub fn short_of_length(self, len: usize) -> Self {
        Self {
            short_len: Some(len),
            ..self
        }
    }

    pub fn to_stack_string(&self) -> StackString<MAX_MULTIHASH_MULTIBASE_REPR_LEN> {
        use std::io::Write;
        let mut buf = [0u8; MAX_MULTIHASH_MULTIBASE_REPR_LEN];

        let len = {
            let mut c = std::io::Cursor::new(&mut buf[..]);
            write!(c, "{self}").unwrap();
            usize::try_from(c.position()).unwrap()
        };

        StackString::new(buf, len)
    }
}

impl std::fmt::Display for MultihashFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = Multibase::encode::<MAX_MULTIHASH_MULTIBASE_REPR_LEN>(
            self.value.as_bytes().as_slice(),
            self.encoding,
        );

        if let Some(short_len) = self.short_len {
            write!(f, "{}", &s[s.len() - short_len..])
        } else {
            write!(f, "{s}")
        }
    }
}

impl std::fmt::Debug for MultihashFmt<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{self}"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "utoipa")]
impl utoipa::ToSchema for Multihash {}

#[cfg(feature = "utoipa")]
impl utoipa::PartialSchema for Multihash {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;

        Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(Type::String))
                .examples([serde_json::json!(Multihash::from_digest_sha3_256(
                    b"example"
                ))])
                .build(),
        )
        .into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
