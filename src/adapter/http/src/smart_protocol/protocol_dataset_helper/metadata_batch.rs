// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::io::Read;

use flate2::Compression;
use futures::TryStreamExt;
use internal_error::{BoxedError, InternalError, ResultIntoInternal};
use tar::Header;
use thiserror::Error;

use crate::smart_protocol::messages::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MEDIA_TAR_GZ: &str = "application/tar+gzip";
const ENCODING_RAW: &str = "raw";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_dataset_metadata_batch(
    metadata_chain: &dyn odf::MetadataChain,
    stop_at: &odf::Multihash,
    begin_after: Option<&odf::Multihash>,
    ignore_missing_tail: bool,
) -> Result<MetadataBlocksBatch, InternalError> {
    let mut num_blocks: u32 = 0;
    let encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
    let mut tarball_builder = tar::Builder::new(encoder);

    let blocks_for_transfer: Vec<odf::dataset::HashedMetadataBlock> = metadata_chain
        .iter_blocks_interval(
            stop_at.into(),
            begin_after.map(Into::into),
            ignore_missing_tail,
        )
        .try_collect()
        .await
        .int_err()?;

    for (hash, _) in blocks_for_transfer.iter().rev() {
        num_blocks += 1;

        let block_bytes = metadata_chain.get_block_bytes(hash).await.int_err()?;

        let block_data: &[u8] = &block_bytes;

        let mut header = Header::new_gnu();
        header.set_size(u64::try_from(block_bytes.len()).unwrap());

        tarball_builder
            .append_data(
                &mut header,
                hash.as_multibase().to_stack_string(),
                block_data,
            )
            .int_err()?;
    }

    let tarball_data = tarball_builder.into_inner().int_err()?.finish().int_err()?;

    Ok(MetadataBlocksBatch {
        num_blocks,
        media_type: String::from(MEDIA_TAR_GZ),
        encoding: String::from(ENCODING_RAW),
        payload: tarball_data,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_metadata_batch(
    blocks_batch: &MetadataBlocksBatch,
) -> Result<VecDeque<odf::dataset::HashedMetadataBlockBytesDecoded>, DecodeMetadataBatchError> {
    let blocks_data = unpack_dataset_metadata_batch(blocks_batch)?;

    let blocks = blocks_data
        .into_iter()
        .map(|(hash, bytes)| {
            // TODO: Avoid depending on specific implementation of
            //       metadata_block_repository_helpers::deserialize_metadata_block.
            //       This is currently necessary because we need to be able to deserialize
            //       blocks BEFORE an instance of MetadataChain exists.
            //       Consider injecting a configurable block deserializer.
            odf::storage::deserialize_metadata_block(&hash, &bytes)
                .map(|block| (hash, block, bytes))
        })
        .collect::<Result<VecDeque<_>, _>>()?;

    Ok(blocks)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A metadata batch received from the other side of a transfer that cannot be
/// read
#[derive(Error, Debug)]
pub enum DecodeMetadataBatchError {
    #[error("Unsupported metadata batch: media type {media_type}, encoding {encoding}")]
    UnsupportedFormat {
        media_type: String,
        encoding: String,
    },

    #[error("Malformed metadata batch")]
    Malformed(#[source] BoxedError),

    #[error(transparent)]
    Block(
        #[from]
        #[backtrace]
        odf::GetBlockError,
    ),
}

impl DecodeMetadataBatchError {
    fn malformed(e: impl Into<BoxedError>) -> Self {
        Self::Malformed(e.into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn unpack_dataset_metadata_batch(
    blocks_batch: &MetadataBlocksBatch,
) -> Result<Vec<(odf::Multihash, odf::MetadataBlockBytes)>, DecodeMetadataBatchError> {
    if blocks_batch.media_type != MEDIA_TAR_GZ || blocks_batch.encoding != ENCODING_RAW {
        return Err(DecodeMetadataBatchError::UnsupportedFormat {
            media_type: blocks_batch.media_type.clone(),
            encoding: blocks_batch.encoding.clone(),
        });
    }

    let decoder = flate2::read::GzDecoder::new(blocks_batch.payload.as_slice());
    let mut archive = tar::Archive::new(decoder);

    let mut blocks_data = Vec::new();
    for entry in archive
        .entries()
        .map_err(DecodeMetadataBatchError::malformed)?
    {
        let mut entry = entry.map_err(DecodeMetadataBatchError::malformed)?;

        let path = entry.path().map_err(DecodeMetadataBatchError::malformed)?;
        let hash = path
            .to_str()
            .ok_or_else(|| DecodeMetadataBatchError::malformed("Entry name is not UTF-8"))
            .and_then(|name| {
                odf::Multihash::from_multibase(name).map_err(DecodeMetadataBatchError::malformed)
            })?;

        let mut buf = Vec::new();
        entry
            .read_to_end(&mut buf)
            .map_err(DecodeMetadataBatchError::malformed)?;

        blocks_data.push((hash, odf::MetadataBlockBytes::new(buf.into())));
    }

    Ok(blocks_data)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::io::Write as _;

    use super::*;

    #[test]
    fn test_decode_metadata_batch_rejects_unsupported_format() {
        let batch = MetadataBlocksBatch {
            num_blocks: 1,
            media_type: String::from("application/zip"),
            encoding: String::from(ENCODING_RAW),
            payload: Vec::new(),
        };

        assert_matches!(
            decode_metadata_batch(&batch),
            Err(DecodeMetadataBatchError::UnsupportedFormat { media_type, .. })
                if media_type == "application/zip"
        );
    }

    #[test]
    fn test_decode_metadata_batch_rejects_payload_that_is_not_a_tarball() {
        let batch = tar_gz_batch(b"not a tarball");

        assert_matches!(
            decode_metadata_batch(&batch),
            Err(DecodeMetadataBatchError::Malformed(_))
        );
    }

    #[test]
    fn test_decode_metadata_batch_rejects_entry_not_named_by_hash() {
        let mut tarball = tar::Builder::new(Vec::new());
        let mut header = Header::new_gnu();
        header.set_size(5);
        header.set_cksum();
        tarball
            .append_data(&mut header, "not-a-hash", b"block".as_slice())
            .unwrap();

        let batch = tar_gz_batch(&tarball.into_inner().unwrap());

        assert_matches!(
            decode_metadata_batch(&batch),
            Err(DecodeMetadataBatchError::Malformed(_))
        );
    }

    fn tar_gz_batch(tarball: &[u8]) -> MetadataBlocksBatch {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(tarball).unwrap();

        MetadataBlocksBatch {
            num_blocks: 1,
            media_type: String::from(MEDIA_TAR_GZ),
            encoding: String::from(ENCODING_RAW),
            payload: encoder.finish().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
