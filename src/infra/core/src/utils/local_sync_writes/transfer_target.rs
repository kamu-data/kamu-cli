// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::transaction;
use kamu_core::{CorruptedSourceError, SyncError, SyncFromTarget, SyncFromTransferredTarget};
use odf::metadata::AsTypedBlock as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Where a pull writes its blocks and objects, and what its commit moves
pub struct TransferTarget {
    /// The dataset storage the transferred blocks and objects go into
    pub storage: Arc<dyn odf::Dataset>,
    pub target: SyncFromTransferredTarget,
}

impl TransferTarget {
    /// Prepares the target of a pull: an existing dataset as is, a new one
    /// stored from the `Seed` that `take_seed` removes from the transferred
    /// blocks
    pub async fn prepare(
        catalog: &dill::Catalog,
        dst: &SyncFromTarget,
        visibility_for_created_dataset: odf::DatasetVisibility,
        take_seed: impl FnOnce() -> odf::dataset::HashedMetadataBlockBytesDecoded,
    ) -> Result<Self, SyncError> {
        match dst {
            SyncFromTarget::Existing {
                dataset,
                expected_head,
            } => Ok(Self {
                storage: (**dataset).clone(),
                target: SyncFromTransferredTarget::Existing {
                    dataset: dataset.clone(),
                    expected_head: expected_head.clone(),
                },
            }),
            SyncFromTarget::New(alias) => {
                Self::store_seed(catalog, alias, visibility_for_created_dataset, take_seed()).await
            }
        }
    }

    /// Stores a new dataset from the source's `Seed`, taking over one an earlier
    /// failed pull left without `HEAD`. Its transaction is its own, so the
    /// transfer protocols hold none, and it writes nothing to the database.
    async fn store_seed(
        catalog: &dill::Catalog,
        alias: &odf::DatasetAlias,
        visibility: odf::DatasetVisibility,
        (seed_hash, seed, seed_block_bytes): odf::dataset::HashedMetadataBlockBytesDecoded,
    ) -> Result<Self, SyncError> {
        let seed_block = seed.into_typed().ok_or_else(|| CorruptedSourceError {
            message: "First metadata block is not Seed".to_owned(),
            source: None,
        })?;

        let stored = transaction!(
            #[catalog(catalog)]
            |writer: Arc<dyn odf::dataset::DatasetStorageUnitWriter>| {
                let stored = writer
                    .store_dataset(
                        seed_block.clone(),
                        odf::dataset::StoreDatasetOpts {
                            seed_block_bytes: Some(seed_block_bytes.clone()),
                        },
                    )
                    .await?;
                stored.dataset.detach_from_transaction();
                stored
            }
        );

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
            storage: stored.dataset,
            target: SyncFromTransferredTarget::New {
                alias: alias.clone(),
                seed_block,
                seed_block_bytes,
                visibility,
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
