// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(non_camel_case_types)]
pub enum Multicodec {
    CIDv1 = 0x01,
    Sha3_256 = 0x16,
    Ed25519Pub = 0xed,
    Arrow0_Sha3_256 = 0x300016,
    // When adding codes don't forget to add them into TryFrom<u32> below
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
            _ => Err(MulticodecError::UnsupportedCode(value)),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum MulticodecError {
    #[error("Unsupported code: {0}")]
    UnsupportedCode(u32),
}
