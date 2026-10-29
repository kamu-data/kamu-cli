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
use kamu_core::{
    CorruptedSourceError,
    DatasetIdMismatchError,
    LocalSyncTarget,
    SyncError,
    TransferredSyncTarget,
};
use odf::dataset::MetadataChainExt as _;
use odf::metadata::AsTypedBlock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset a pull created in storage only: its `Seed` is written, it has
/// no `HEAD` and no entry until the transfer is committed
struct StoredSeed {
    dataset: Arc<dyn odf::Dataset>,
    seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
    seed_block_bytes: odf::MetadataBlockBytes,
}

impl StoredSeed {
    /// Stores a new dataset from the first block a source sent. Storage takes
    /// over a dataset left without `HEAD` by an earlier failed pull. The short
    /// transaction writes nothing to the database: database-backed storage
    /// only needs one to build the dataset, which is detached from it on return
    async fn store(
        catalog: &dill::Catalog,
        (seed_hash, seed, seed_block_bytes): odf::dataset::HashedMetadataBlockBytesDecoded,
    ) -> Result<Self, SyncError> {
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

/// Checks that a source written into an existing dataset without `HEAD`, whose
/// whole chain is transferred, starts with that dataset's `Seed`: nothing else
/// compares the dataset IDs when the destination has no blocks to compare
pub fn ensure_seed_matches_headless_target(
    dst: &LocalSyncTarget,
    oldest_block: &odf::dataset::HashedMetadataBlockBytesDecoded,
) -> Result<(), SyncError> {
    let LocalSyncTarget::Existing {
        dataset,
        expected_head: None,
    } = dst
    else {
        return Ok(());
    };

    let (_, block, _) = oldest_block;
    let Some(seed) = block.as_typed::<odf::metadata::Seed>() else {
        return Err(CorruptedSourceError {
            message: "First metadata block is not Seed".to_owned(),
            source: None,
        }
        .into());
    };

    let dst_dataset_id = dataset.get_id();
    if seed.event.dataset_id != *dst_dataset_id {
        return Err(DatasetIdMismatchError {
            src_dataset_id: seed.event.dataset_id.clone(),
            dst_dataset_id: dst_dataset_id.clone(),
        }
        .into());
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Where a pull writes its blocks and objects, and what its commit moves
pub struct TransferTarget {
    /// The dataset storage the transferred blocks and objects go into
    pub storage: Arc<dyn odf::Dataset>,
    pub target: TransferredSyncTarget,
}

/// Prepares the target of a pull: an existing dataset as is, a new one stored
/// from the `Seed` that `take_seed` removes from the transferred blocks
pub async fn prepare_transfer_target(
    catalog: &dill::Catalog,
    dst: &LocalSyncTarget,
    visibility_for_created_dataset: odf::DatasetVisibility,
    take_seed: impl FnOnce() -> odf::dataset::HashedMetadataBlockBytesDecoded,
) -> Result<TransferTarget, SyncError> {
    match dst {
        LocalSyncTarget::Existing {
            dataset,
            expected_head,
        } => Ok(TransferTarget {
            storage: (**dataset).clone(),
            target: TransferredSyncTarget::Existing {
                dataset: dataset.clone(),
                expected_head: expected_head.clone(),
            },
        }),
        LocalSyncTarget::New(alias) => {
            let StoredSeed {
                dataset,
                seed_block,
                seed_block_bytes,
            } = StoredSeed::store(catalog, take_seed()).await?;
            Ok(TransferTarget {
                storage: dataset,
                target: TransferredSyncTarget::New {
                    alias: alias.clone(),
                    seed_block,
                    seed_block_bytes,
                    visibility: visibility_for_created_dataset,
                },
            })
        }
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
