// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::ResultIntoInternal;
use odf_dataset::*;
use odf_metadata::{DatasetID, DatasetSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn create_test_dataset_from_snapshot(
    dataset_handle_resolver: &dyn DatasetHandleResolver,
    storage_unit_writer: &dyn DatasetStorageUnitWriter,
    mut snapshot: DatasetSnapshot,
    dataset_id: DatasetID,
    system_time: DateTime<Utc>,
) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
    // Validate / resolve metadata events from the snapshot
    normalize_and_validate_dataset_snapshot(dataset_handle_resolver, &mut snapshot).await?;

    // Create dataset in the storage unit
    let create_dataset_result = storage_unit_writer
        .create_dataset(
            &snapshot.name,
            make_seed_block(dataset_id, snapshot.kind, system_time),
        )
        .await?;

    // Append snapshot metadata
    let append_result = match append_snapshot_metadata_to_dataset(
        snapshot.metadata,
        create_dataset_result.dataset.as_ref(),
        &create_dataset_result.head,
        system_time,
    )
    .await
    {
        Ok(append_result) => Ok(append_result),
        Err(e) => {
            // Attempt to clean up dataset
            let _ = storage_unit_writer
                .delete_dataset(&create_dataset_result.dataset_handle)
                .await;
            Err(e)
        }
    }?;

    // Commit HEAD
    let chain = create_dataset_result.dataset.as_metadata_chain();
    chain
        .set_ref(
            &BlockRef::Head,
            &append_result.proposed_head,
            SetRefOpts {
                validate_block_present: false,
                check_ref_is: Some(Some(&create_dataset_result.head)),
            },
        )
        .await
        .int_err()?;

    Ok(CreateDatasetResult {
        head: append_result.proposed_head,
        ..create_dataset_result
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
