// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf::serde::flatbuffers::FlatbuffersMetadataBlockDeserializer;
use odf::serde::{Error, MetadataBlockDeserializer};
use odf_metadata as odf;

use crate::{BlockMalformedError, BlockVersionError, GetBlockError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn deserialize_metadata_block(
    hash: &odf::Multihash,
    block_bytes: &[u8],
) -> Result<odf::MetadataBlock, GetBlockError> {
    FlatbuffersMetadataBlockDeserializer
        .read_manifest(block_bytes)
        .map_err(|e| match e {
            Error::UnsupportedVersion { .. } => GetBlockError::BlockVersion(BlockVersionError {
                hash: hash.clone(),
                source: e.into(),
            }),
            _ => GetBlockError::BlockMalformed(BlockMalformedError {
                hash: hash.clone(),
                source: e.into(),
            }),
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
