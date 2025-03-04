// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, Catalog};
use internal_error::ResultIntoInternal;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{DatasetRegistry, DidGenerator};
use kamu_datasets::{
    CreateDatasetFromSnapshotError,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    CreateDatasetUseCaseOptions,
};
use time_source::SystemTimeSource;

use crate::utils::CreateDatasetUseCaseHelper;
use crate::DependencyGraphWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CreateDatasetFromSnapshotUseCase)]
pub struct CreateDatasetFromSnapshotUseCaseImpl {
    catalog: Catalog,
    current_account_subject: Arc<CurrentAccountSubject>,
    system_time_source: Arc<dyn SystemTimeSource>,
    did_generator: Arc<dyn DidGenerator>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dependency_graph_writer: Arc<dyn DependencyGraphWriter>,
    create_helper: Arc<CreateDatasetUseCaseHelper>,
}

impl CreateDatasetFromSnapshotUseCaseImpl {
    pub fn new(
        catalog: Catalog,
        current_account_subject: Arc<CurrentAccountSubject>,
        system_time_source: Arc<dyn SystemTimeSource>,
        did_generator: Arc<dyn DidGenerator>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dependency_graph_writer: Arc<dyn DependencyGraphWriter>,
        create_helper: Arc<CreateDatasetUseCaseHelper>,
    ) -> Self {
        Self {
            catalog,
            current_account_subject,
            system_time_source,
            did_generator,
            dataset_registry,
            dependency_graph_writer,
            create_helper,
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
            )
            .await?;

        // Make storage level dataset (no HEAD yet)
        let store_result = self
            .create_helper
            .store_dataset(&canonical_alias, seed_block)
            .await?;

        // Analyze dependencies
        let mut new_upstream_ids: Vec<odf::DatasetID> = vec![];
        for event in &snapshot.metadata {
            if let odf::MetadataEvent::SetTransform(set_transform) = event {
                // Collect only the latest upstream dataset IDs
                new_upstream_ids.clear();
                for new_input in &set_transform.inputs {
                    if let Some(id) = new_input.dataset_ref.id() {
                        new_upstream_ids.push(id.clone());
                    } else {
                        // Input references must be resolved to IDs here, but we
                        // ignore the errors and let the metadata chain reject
                        // this event
                    }
                }
            }
        }

        // Append snapshot metadata
        let append_result = odf::dataset::append_snapshot_metadata_to_dataset(
            snapshot.metadata,
            store_result.dataset.as_ref(),
            &store_result.seed,
            system_time,
        )
        .await
        .int_err()?;

        // Set initial dataset HEAD
        self.create_helper
            .set_created_head(&store_result.dataset_id, &append_result.proposed_head)
            .await?;

        // Note: modify dependencies only after `set_ref` succeeds.
        // TODO: the dependencies should be updated as a part of HEAD change
        if !new_upstream_ids.is_empty() {
            self.dependency_graph_writer
                .update_dataset_node_dependencies(
                    &self.catalog,
                    &store_result.dataset_id,
                    new_upstream_ids,
                )
                .await?;
        }

        // Notify interested parties the dataset was created
        self.create_helper
            .notify_dataset_created(
                &store_result.dataset_id,
                &canonical_alias.dataset_name,
                &logged_account_id,
                options.dataset_visibility,
            )
            .await?;

        Ok(CreateDatasetResult {
            head: append_result.proposed_head,
            dataset: store_result.dataset,
            dataset_handle: odf::DatasetHandle::new(store_result.dataset_id, canonical_alias),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
