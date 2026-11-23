// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use odf_dataset::*;
use odf_metadata::*;
use odf_storage::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Fails with a ref collision when `storage_unit` already holds a dataset with
/// this ID and a `HEAD`. A stored dataset without `HEAD` was left by an
/// interrupted creation and not collected yet, so storing takes it over
pub(crate) async fn ensure_no_stored_head(
    storage_unit: &impl DatasetStorageUnit,
    dataset_id: &DatasetID,
) -> Result<(), StoreDatasetError> {
    let existing_dataset = match storage_unit.get_stored_dataset_by_id(dataset_id).await {
        Ok(existing_dataset) => existing_dataset,
        Err(GetStoredDatasetError::UnresolvedId(_)) => return Ok(()),
        Err(GetStoredDatasetError::Internal(e)) => return Err(StoreDatasetError::Internal(e)),
    };

    match existing_dataset
        .as_metadata_chain()
        .as_uncached_ref_repo()
        .get(BlockRef::Head.as_str())
        .await
    {
        Ok(_) => Err(StoreDatasetError::RefCollision(RefCollisionError {
            id: dataset_id.clone(),
        })),
        Err(GetRefError::NotFound(_)) => {
            tracing::info!(
                %dataset_id,
                "Taking over a stored dataset left without HEAD"
            );
            Ok(())
        }
        Err(GetRefError::Access(e)) => Err(StoreDatasetError::Internal(e.int_err())),
        Err(GetRefError::Internal(e)) => Err(StoreDatasetError::Internal(e)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Appends the seed block to a new dataset's chain without setting `HEAD`; the
/// caller sets it
pub(crate) async fn append_seed(
    metadata_chain: &dyn MetadataChain,
    seed_block: MetadataBlockTyped<Seed>,
    opts: &StoreDatasetOpts,
) -> Result<Multihash, StoreDatasetError> {
    metadata_chain
        .append(
            seed_block.into(),
            AppendOpts {
                update_ref: None,
                block_data: opts
                    .seed_block_bytes
                    .as_ref()
                    .map(MetadataBlockBytes::as_slice),
                ..AppendOpts::default()
            },
        )
        .await
        .int_err()
        .map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
