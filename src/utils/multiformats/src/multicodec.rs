// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub enum Multicodec {
    CIDv1 = 0x01,
    Sha2_256 = 0x12,
    Sha3_256 = 0x16,
    Ed25519Pub = 0xed,
    // -- Private use area --
    // Arrow hashes range
    Arrow0_Sha3_256 = 0x0030_0016,
    // ODF resources range
    ODFMetadataBlock = 0x0040_0000,
    ODFDatasetSnapshot = 0x0040_0001,
    // When adding codes don't forget to add them into traits below
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: use num-derive
impl TryFrom<u32> for Multicodec {
    type Error = MulticodecUnsupportedCode;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Multicodec::CIDv1),
            0x12 => Ok(Multicodec::Sha2_256),
            0x16 => Ok(Multicodec::Sha3_256),
            0xed => Ok(Multicodec::Ed25519Pub),
            0x0030_0016 => Ok(Multicodec::Arrow0_Sha3_256),
            0x0040_0000 => Ok(Multicodec::ODFMetadataBlock),
            0x0040_0001 => Ok(Multicodec::ODFDatasetSnapshot),
            _ => Err(MulticodecUnsupportedCode(value)),
        }
    }
}

impl std::str::FromStr for Multicodec {
    type Err = MulticodecUnsupportedName;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Align with official multicodec names: https://github.com/multiformats/multicodec
        match s {
            "cidv1" => Ok(Multicodec::CIDv1),
            "sha2-256" => Ok(Multicodec::Sha2_256),
            "sha3-256" => Ok(Multicodec::Sha3_256),
            "ed25519-pub" => Ok(Multicodec::Ed25519Pub),
            "arrow0-sha3-256" => Ok(Multicodec::Arrow0_Sha3_256),
            "odf-metadata-block" => Ok(Multicodec::ODFMetadataBlock),
            "odf-dataset-snapshot" => Ok(Multicodec::ODFDatasetSnapshot),
            _ => Err(MulticodecUnsupportedName(s.into())),
        }
    }
}

impl TryFrom<&str> for Multicodec {
    type Error = MulticodecUnsupportedName;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        str::parse(value)
    }
}

impl std::fmt::Display for Multicodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Multicodec::CIDv1 => "cidv1",
            Multicodec::Sha2_256 => "sha2-256",
            Multicodec::Sha3_256 => "sha3-256",
            Multicodec::Ed25519Pub => "ed25519-pub",
            Multicodec::Arrow0_Sha3_256 => "arrow0-sha3-256",
            Multicodec::ODFMetadataBlock => "odf-metadata-block",
            Multicodec::ODFDatasetSnapshot => "odf-dataset-snapshot",
        };
        write!(f, "{s}")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Unsupported multicodec code '{0:#x}'")]
pub struct MulticodecUnsupportedCode(u32);

#[derive(thiserror::Error, Debug)]
#[error("Unsupported multicodec name '{0}'")]
pub struct MulticodecUnsupportedName(String);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl serde::Serialize for Multicodec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Multicodec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_string(MulticodecSerdeVisitor)
    }
}

struct MulticodecSerdeVisitor;

impl serde::de::Visitor<'_> for MulticodecSerdeVisitor {
    type Value = Multicodec;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a Multicodec string")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Multicodec::try_from(v).map_err(serde::de::Error::custom)
    }
}
