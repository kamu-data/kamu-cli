// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::ResultIntoInternal;
use kamu_core::{DatasetRegistry, DidGenerator};
use kamu_datasets::{
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    DatasetLifecycleMessage,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CreateDatasetFromSnapshotUseCase)]
pub struct CreateDatasetFromSnapshotUseCaseImpl {
    create_dataset_use_case: Arc<dyn CreateDatasetUseCase>,
    system_time_source: Arc<dyn SystemTimeSource>,
    did_generator: Arc<dyn DidGenerator>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    outbox: Arc<dyn Outbox>,
}

impl CreateDatasetFromSnapshotUseCaseImpl {
    pub fn new(
        create_dataset_use_case: Arc<dyn CreateDatasetUseCase>,
        system_time_source: Arc<dyn SystemTimeSource>,
        did_generator: Arc<dyn DidGenerator>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            create_dataset_use_case,
            system_time_source,
            did_generator,
            dataset_registry,
            dataset_storage_unit_writer,
            outbox,
        }
    }
}

#[async_trait::async_trait]
impl CreateDatasetFromSnapshotUseCase for CreateDatasetFromSnapshotUseCaseImpl {
    #[tracing::instrument(level = "info", skip_all, fields(?snapshot, ?options))]
    async fn execute(
        &self,
        mut snapshot: odf::DatasetSnapshot,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<odf::CreateDatasetResult, odf::dataset::CreateDatasetFromSnapshotError> {
        // Validate / resolve metadata events from the snapshot
        odf::dataset::normalize_and_validate_dataset_snapshot(
            self.dataset_registry.as_ref(),
            &mut snapshot,
        )
        .await?;

        // Create new clean dataset
        let system_time = self.system_time_source.now();
        let create_dataset_result = self
            .create_dataset_use_case
            .execute(
                &snapshot.name,
                odf::dataset::make_seed_block(
                    self.did_generator.generate_dataset_id(),
                    snapshot.kind,
                    system_time,
                ),
                options,
            )
            .await?;

        // Append snapshot metadata
        let append_result = match odf::dataset::append_metadata_to_dataset(
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
                let _ = self
                    .dataset_storage_unit_writer
                    .delete_dataset(&create_dataset_result.dataset_handle)
                    .await;
                Err(e)
            }
        }?;

        // Commit HEAD
        let chain = create_dataset_result.dataset.as_metadata_chain();
        chain
            .set_ref(
                &odf::BlockRef::Head,
                &append_result.new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: false,
                    check_ref_is: Some(Some(&create_dataset_result.head)),
                },
            )
            .await
            .int_err()?;

        // Notify of dependencies
        if !append_result.new_upstream_ids.is_empty() {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                    DatasetLifecycleMessage::dependencies_updated(
                        create_dataset_result.dataset_handle.id.clone(),
                        append_result.new_upstream_ids,
                    ),
                )
                .await?;
        }

        Ok(create_dataset_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
