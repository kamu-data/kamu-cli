// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use odf_dataset::*;
use odf_metadata::{DatasetID, DatasetSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn create_test_dataset_from_snapshot(
    dataset_handle_resolver: &dyn DatasetHandleResolver,
    storage_unit_writer: &dyn DatasetStorageUnitWriter,
    mut snapshot: DatasetSnapshot,
    dataset_id: DatasetID,
    system_time: DateTime<Utc>,
) -> Result<StoreDatasetResult, InternalError> {
    // Validate / resolve metadata events from the snapshot
    normalize_and_validate_dataset_snapshot(dataset_handle_resolver, &mut snapshot)
        .await
        .int_err()?;

    // Remember alias
    let alias = snapshot.name.clone();

    // Create dataset in the storage unit
    let store_dataset_result = storage_unit_writer
        .store_dataset(make_seed_block(dataset_id, snapshot.kind, system_time))
        .await
        .int_err()?;

    // Append snapshot metadata
    let append_result = match append_snapshot_metadata_to_dataset(
        snapshot.metadata,
        store_dataset_result.dataset.as_ref(),
        &store_dataset_result.head,
        system_time,
    )
    .await
    {
        Ok(append_result) => Ok(append_result),
        Err(e) => {
            // Attempt to clean up dataset
            let _ = storage_unit_writer
                .delete_dataset(&store_dataset_result.dataset_id)
                .await;
            Err(e)
        }
    }
    .int_err()?;

    // Commit HEAD
    let chain = store_dataset_result.dataset.as_metadata_chain();
    chain
        .set_ref(
            &BlockRef::Head,
            &append_result.proposed_head,
            SetRefOpts {
                validate_block_present: false,
                check_ref_is: Some(Some(&store_dataset_result.head)),
            },
        )
        .await
        .int_err()?;

    write_dataset_alias(store_dataset_result.dataset.as_ref(), &alias)
        .await
        .unwrap();

    Ok(StoreDatasetResult {
        head: append_result.proposed_head,
        ..store_dataset_result
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
