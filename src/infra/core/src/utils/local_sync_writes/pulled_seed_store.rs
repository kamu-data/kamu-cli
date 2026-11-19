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
use kamu_core::{CorruptedSourceError, SyncError};
use odf::metadata::AsTypedBlock as _;

use super::stored_seed::StoredSeed;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Stores the `Seed` of a dataset a pull creates, in a transaction of its own,
/// so the transfer protocols that call it hold no transaction
#[dill::component(pub)]
pub struct PulledSeedStore {
    catalog: dill::Catalog,
}

impl PulledSeedStore {
    /// Stores a new dataset from the source's `Seed`, taking over one an earlier
    /// failed pull left without `HEAD`. Writes nothing to the database.
    pub(super) async fn store(
        &self,
        (seed_hash, seed, seed_block_bytes): odf::dataset::HashedMetadataBlockBytesDecoded,
    ) -> Result<StoredSeed, SyncError> {
        let seed_block = seed.into_typed().ok_or_else(|| CorruptedSourceError {
            message: "First metadata block is not Seed".to_owned(),
            source: None,
        })?;

        let stored = transaction!(|writer: Arc<dyn odf::dataset::DatasetStorageUnitWriter>| {
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
        });

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

        Ok(StoredSeed {
            dataset: stored.dataset,
            seed_block,
            seed_block_bytes,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
