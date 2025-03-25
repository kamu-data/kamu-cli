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
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{DatasetRegistry, DidGenerator, ResolvedDataset};
use kamu_datasets::{
    CreateDatasetFromSnapshotError,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    CreateDatasetUseCaseOptions,
};
use time_source::SystemTimeSource;

use crate::utils::CreateDatasetUseCaseHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn CreateDatasetFromSnapshotUseCase)]
pub struct CreateDatasetFromSnapshotUseCaseImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    system_time_source: Arc<dyn SystemTimeSource>,
    did_generator: Arc<dyn DidGenerator>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    create_helper: Arc<CreateDatasetUseCaseHelper>,
}

#[async_trait::async_trait]
impl CreateDatasetFromSnapshotUseCase for CreateDatasetFromSnapshotUseCaseImpl {
    #[tracing::instrument(level = "info", skip_all, fields(?snapshot, ?options))]
    async fn execute(
        &self,
        mut snapshot: odf::DatasetSnapshot,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        // There must be a logged in user
        let logged_account_id = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account cannot create dataset");
            }
            CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        };

        // Validate / resolve metadata events from the snapshot
        odf::dataset::normalize_and_validate_dataset_snapshot(
            self.dataset_registry.as_ref(),
            &mut snapshot,
        )
        .await?;

        // Adjust alias for current tenancy configuration
        let canonical_alias = self.create_helper.canonical_dataset_alias(&snapshot.name);

        // Make a seed block
        let system_time = self.system_time_source.now();
        let seed_block = odf::dataset::make_seed_block(
            self.did_generator.generate_dataset_id().0,
            snapshot.kind,
            system_time,
        );

        // Dataset entry goes first, this guarantees name collision check
        self.create_helper
            .create_dataset_entry(
                &seed_block.event.dataset_id,
                &logged_account_id,
                &canonical_alias,
                snapshot.kind,
            )
            .await?;

        // Make storage level dataset (no HEAD yet)
        let store_result = self
            .create_helper
            .store_dataset(&canonical_alias, seed_block)
            .await?;

        // Append snapshot metadata
        let append_result = odf::dataset::append_snapshot_metadata_to_dataset(
            snapshot.metadata,
            store_result.dataset.as_ref(),
            &store_result.seed,
            system_time,
        )
        .await
        .int_err()?;

        // Notify interested parties the dataset was created
        self.create_helper
            .notify_dataset_created(
                &store_result.dataset_id,
                &canonical_alias.dataset_name,
                &logged_account_id,
                options.dataset_visibility,
            )
            .await?;

        // Set initial dataset HEAD
        self.create_helper
            .set_created_head(
                ResolvedDataset::from_stored(&store_result, &canonical_alias),
                &append_result.proposed_head,
            )
            .await?;

        Ok(CreateDatasetResult {
            head: append_result.proposed_head,
            dataset: store_result.dataset,
            dataset_handle: odf::DatasetHandle::new(store_result.dataset_id, canonical_alias),
            dataset_kind: store_result.dataset_kind,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
