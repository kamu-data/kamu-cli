// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::{SyncError, SyncFromTarget, SyncFromTransferredTarget};

use super::stored_seed::StoredSeed;

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
                let StoredSeed {
                    dataset,
                    seed_block,
                    seed_block_bytes,
                } = StoredSeed::store(catalog, take_seed()).await?;
                Ok(Self {
                    storage: dataset,
                    target: SyncFromTransferredTarget::New {
                        alias: alias.clone(),
                        seed_block,
                        seed_block_bytes,
                        visibility: visibility_for_created_dataset,
                    },
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
