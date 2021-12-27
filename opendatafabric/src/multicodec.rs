// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::InvalidValue;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub enum Multicodec {
    CIDv1 = 0x01,
    Sha3_256 = 0x16,
    Ed25519Pub = 0xed,
    // -- Private use area --
    // Arrow hashes range
    Arrow0_Sha3_256 = 0x300016,
    // ODF resources range
    ODFMetadataBlock = 0x400000,
    ODFDatasetSnapshot = 0x400001,
    // When adding codes don't forget to add them into traits below
}

///////////////////////////////////////////////////////////////////////////////

// TODO: use num-derive
impl TryFrom<u32> for Multicodec {
    type Error = MulticodecError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Multicodec::CIDv1),
            0x16 => Ok(Multicodec::Sha3_256),
            0xed => Ok(Multicodec::Ed25519Pub),
            0x300016 => Ok(Multicodec::Arrow0_Sha3_256),
            0x400000 => Ok(Multicodec::ODFMetadataBlock),
            0x400001 => Ok(Multicodec::ODFDatasetSnapshot),
            _ => Err(MulticodecError::UnsupportedCode(value)),
        }
    }
}

impl std::str::FromStr for Multicodec {
    type Err = InvalidValue<Multicodec>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Align with official multicodec names: https://github.com/multiformats/multicodec
        match s {
            "cidv1" => Ok(Multicodec::CIDv1),
            "sha3-256" => Ok(Multicodec::Sha3_256),
            "ed25519-pub" => Ok(Multicodec::Ed25519Pub),
            "arrow0-sha3-256" => Ok(Multicodec::Arrow0_Sha3_256),
            "odf-metadata-block" => Ok(Multicodec::ODFMetadataBlock),
            "odf-dataset-snapshot" => Ok(Multicodec::ODFDatasetSnapshot),
            _ => Err(InvalidValue::new(s)),
        }
    }
}

super::impl_try_from_str!(Multicodec);

impl std::fmt::Display for Multicodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Multicodec::CIDv1 => "cidv1",
            Multicodec::Sha3_256 => "sha3-256",
            Multicodec::Ed25519Pub => "ed25519-pub",
            Multicodec::Arrow0_Sha3_256 => "arrow0-sha3-256",
            Multicodec::ODFMetadataBlock => "odf-metadata-block",
            Multicodec::ODFDatasetSnapshot => "odf-dataset-snapshot",
        };
        write!(f, "{}", s)
    }
}

///////////////////////////////////////////////////////////////////////////////

super::impl_invalid_value!(Multicodec);

#[derive(Error, Debug)]
pub enum MulticodecError {
    #[error("Unsupported code: {0}")]
    UnsupportedCode(u32),
}

///////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for Multicodec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Multicodec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = deserializer.deserialize_string(MulticodecSerdeVisitor)?;
        Self::try_from(s).map_err(serde::de::Error::custom)
    }
}

struct MulticodecSerdeVisitor;

impl<'de> serde::de::Visitor<'de> for MulticodecSerdeVisitor {
    type Value = Multicodec;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Multicodec string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Multicodec::try_from(v).map_err(serde::de::Error::custom)
    }
}
