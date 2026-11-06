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
use kamu_core::{CorruptedSourceError, SyncError};
use odf::metadata::AsTypedBlock as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset a pull created in storage only: its `Seed` is written, it has
/// no `HEAD` and no dataset entry until the transfer is committed
pub(super) struct StoredSeed {
    pub dataset: Arc<dyn odf::Dataset>,
    pub seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
    pub seed_block_bytes: odf::MetadataBlockBytes,
}

impl StoredSeed {
    /// Stores a new dataset from the source's `Seed`, taking over one an earlier
    /// failed pull left without `HEAD`. Writes nothing to the database.
    pub(super) async fn store(
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
