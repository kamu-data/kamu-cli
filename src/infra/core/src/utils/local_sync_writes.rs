// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::DatabaseTransactionRunner;
use internal_error::ErrorIntoInternal;
use kamu_core::{CorruptedSourceError, SyncError};
use odf::dataset::MetadataChainExt as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset a pull created in storage only: its `Seed` is written, it has
/// no `HEAD` and no entry until the transfer is committed
pub struct StoredSeed {
    pub dataset: Arc<dyn odf::Dataset>,
    pub seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
    pub seed_block_bytes: odf::MetadataBlockBytes,
}

impl StoredSeed {
    /// Stores a new dataset from the first block a source sent. Storage takes
    /// over a dataset left without `HEAD` by an earlier failed pull. The short
    /// transaction writes nothing to the database: database-backed storage
    /// only needs one to build the dataset, which is detached from it on return
    pub async fn store(
        catalog: &dill::Catalog,
        (seed_hash, seed, seed_block_bytes): odf::dataset::HashedMetadataBlockBytesDecoded,
    ) -> Result<Self, SyncError> {
        use odf::metadata::AsTypedBlock;

        let seed_block = seed.into_typed().ok_or_else(|| CorruptedSourceError {
            message: "First metadata block is not Seed".to_owned(),
            source: None,
        })?;

        let stored = DatabaseTransactionRunner::new(catalog.clone())
            .transactional_with(
                |dataset_storage_unit_writer: Arc<dyn odf::dataset::DatasetStorageUnitWriter>| {
                    let seed_block = seed_block.clone();
                    let seed_block_bytes = seed_block_bytes.clone();
                    async move {
                        let stored = dataset_storage_unit_writer
                            .store_dataset(
                                seed_block,
                                odf::dataset::StoreDatasetOpts {
                                    seed_block_bytes: Some(seed_block_bytes),
                                },
                            )
                            .await
                            .map_err(|e| match e {
                                odf::dataset::StoreDatasetError::RefCollision(e) => {
                                    SyncError::RefCollision(e)
                                }
                                odf::dataset::StoreDatasetError::Internal(e) => {
                                    SyncError::Internal(e)
                                }
                            })?;
                        stored.dataset.detach_from_transaction();
                        Ok::<_, SyncError>(stored)
                    }
                },
            )
            .await?;

        if stored.seed != seed_hash {
            return Err(CorruptedSourceError {
                message: format!(
                    "Seed block hash declared by the source {seed_hash} didn't match the computed \
                     {} - this may be an indication of hashing algorithm mismatch or an attempt \
                     to tamper data",
                    stored.seed
                ),
                source: None,
            }
            .into());
        }

        Ok(Self {
            dataset: stored.dataset,
            seed_block,
            seed_block_bytes,
        })
    }
}

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

pub(crate) fn map_append_batch_error(
    odf::dataset::AppendBatchError {
        append_error,
        block_sequence_number,
        block_hash,
    }: odf::dataset::AppendBatchError,
) -> SyncError {
    use odf::dataset::{AppendError, AppendValidationError};

    match append_error {
        AppendError::InvalidBlock(append_validation_error) => {
            let message = match &append_validation_error {
                AppendValidationError::HashMismatch(e) => {
                    format!(
                        "Block hash declared by the source {} didn't match the computed {} at \
                         block {block_sequence_number} - this may be an indication of hashing \
                         algorithm mismatch or an attempt to tamper data",
                        e.actual, e.expected
                    )
                }
                AppendValidationError::FirstBlockMustBeSeed
                | AppendValidationError::AppendingSeedBlockToNonEmptyChain
                | AppendValidationError::PrevBlockNotFound(_)
                | AppendValidationError::SequenceIntegrity(_)
                | AppendValidationError::SystemTimeIsNotMonotonic
                | AppendValidationError::WatermarkIsNotMonotonic
                | AppendValidationError::OffsetsAreNotSequential(_)
                | AppendValidationError::InvalidEvent(_)
                | AppendValidationError::NoOpEvent(_) => format!(
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
        // A batch is appended without moving a reference
        AppendError::RefNotFound(_) | AppendError::RefCASFailed(_) => unreachable!(),
        AppendError::Access(e) => SyncError::Access(e),
        e @ AppendError::Internal(_) => SyncError::Internal(e.int_err()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
