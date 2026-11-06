// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{CorruptedSourceError, DatasetIdMismatchError, LocalSyncTarget, SyncError};
use odf::metadata::AsTypedBlock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checks that a source written into an existing dataset without `HEAD`, whose
/// whole chain is transferred, starts with that dataset's `Seed`: nothing else
/// compares the dataset IDs when the destination has no blocks to compare
pub(crate) fn ensure_seed_matches_headless_target(
    dst: &LocalSyncTarget,
    oldest_block: &odf::dataset::HashedMetadataBlockBytesDecoded,
) -> Result<(), SyncError> {
    let dataset = match dst {
        LocalSyncTarget::Existing {
            dataset,
            expected_head: None,
        } => dataset,
        // Chains with a head are compared from it; a new dataset is stored from
        // the source's own `Seed`
        LocalSyncTarget::Existing {
            expected_head: Some(_),
            ..
        }
        | LocalSyncTarget::New(_) => return Ok(()),
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

    tracing::info!(
        dataset_id = %dst_dataset_id,
        "Writing the whole source chain into a dataset left without HEAD"
    );
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
