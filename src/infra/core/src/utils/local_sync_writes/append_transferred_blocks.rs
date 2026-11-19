// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::ErrorIntoInternal;
use kamu_core::{CorruptedSourceError, SyncError};
use odf::dataset::MetadataChainExt as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Appends transferred blocks, oldest first, without moving any reference
pub async fn append_transferred_blocks(
    dst: &dyn odf::Dataset,
    blocks: Box<dyn Iterator<Item = odf::dataset::HashedMetadataBlockBytesDecoded> + Send>,
    opts: odf::dataset::AppendBatchOpts,
) -> Result<(), SyncError> {
    dst.as_metadata_chain()
        .append_batch(blocks, opts)
        .await
        .map_err(map_append_batch_error)?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_append_batch_error(
    odf::dataset::AppendBatchError {
        append_error,
        block_sequence_number,
        block_hash,
    }: odf::dataset::AppendBatchError,
) -> SyncError {
    use odf::dataset::{AppendError as E, AppendValidationError as V};

    match append_error {
        E::InvalidBlock(append_validation_error) => {
            let message = match &append_validation_error {
                V::HashMismatch(e) => {
                    format!(
                        "Block hash declared by the source {} didn't match the computed {} at \
                         block {block_sequence_number} - this may be an indication of hashing \
                         algorithm mismatch or an attempt to tamper data",
                        e.expected, e.actual
                    )
                }
                V::FirstBlockMustBeSeed
                | V::AppendingSeedBlockToNonEmptyChain
                | V::PrevBlockNotFound(_)
                | V::SequenceIntegrity(_)
                | V::SystemTimeIsNotMonotonic
                | V::WatermarkIsNotMonotonic
                | V::OffsetsAreNotSequential(_)
                | V::InvalidEvent(_)
                | V::NoOpEvent(_) => format!(
                    "Source metadata chain is logically inconsistent at block \
                     {block_hash}[{block_sequence_number}]"
                ),
            };

            CorruptedSourceError {
                message,
                source: Some(append_validation_error.into()),
            }
            .into()
        }
        E::Access(e) => SyncError::Access(e),
        // A batch is appended without moving a reference, so a reference error is unexpected
        e @ (E::RefNotFound(_) | E::RefCASFailed(_) | E::Internal(_)) => {
            SyncError::Internal(e.int_err())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
