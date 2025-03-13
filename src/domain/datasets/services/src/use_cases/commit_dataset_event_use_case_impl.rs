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
use internal_error::ErrorIntoInternal;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::DatasetRegistry;
use kamu_datasets::{CommitDatasetEventUseCase, ViewMultiResponse};
use odf::dataset::{AppendError, InvalidEventError};
use odf::metadata::EnumWithVariants;

use crate::utils::access_dataset_helper::{AccessDatasetHelper, DatasetAccessError};
use crate::DependencyGraphWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CommitDatasetEventUseCase)]
pub struct CommitDatasetEventUseCaseImpl {
    catalog: Catalog,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dependency_graph_writer: Arc<dyn DependencyGraphWriter>,
}

impl CommitDatasetEventUseCaseImpl {
    pub fn new(
        catalog: Catalog,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        dependency_graph_writer: Arc<dyn DependencyGraphWriter>,
    ) -> Self {
        Self {
            catalog,
            dataset_registry,
            dataset_action_authorizer,
            dependency_graph_writer,
        }
    }

    async fn validate_event(
        &self,
        access_dataset_helper: AccessDatasetHelper<'_>,
        event: odf::MetadataEvent,
    ) -> Result<odf::MetadataEvent, odf::dataset::CommitError> {
        if let Some(set_transform) = event.as_variant::<odf::metadata::SetTransform>() {
            let inputs_dataset_refs = set_transform
                .inputs
                .iter()
                .map(|input| input.dataset_ref.clone())
                .collect::<Vec<_>>();

            let view_multi_result: ViewMultiResponse = access_dataset_helper
                .access_multi_dataset(inputs_dataset_refs, DatasetAction::Read)
                .await
                .map(Into::into)?;

            if !view_multi_result.inaccessible_refs.is_empty() {
                let dataset_ref_alias_map = set_transform.as_dataset_ref_alias_map();
                let message = view_multi_result
                    .into_inaccessible_input_datasets_message(&dataset_ref_alias_map);

                return Err(AppendError::InvalidBlock(
                    InvalidEventError::new(event, message).into(),
                )
                .into());
            }
        }

        Ok(event)
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CommitDatasetEventUseCase for CommitDatasetEventUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = CommitDatasetEventUseCaseImpl_execute,
        skip_all,
        fields(dataset_handle, ?event)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        event: odf::MetadataEvent,
    ) -> Result<odf::dataset::CommitResult, odf::dataset::CommitError> {
        let access_dataset_helper =
            AccessDatasetHelper::new(&self.dataset_registry, &self.dataset_action_authorizer);

        access_dataset_helper
            .access_dataset(&dataset_handle.as_local_ref(), DatasetAction::Write)
            .await
            .map_err(|e| {
                use odf::dataset::CommitError;
                match e {
                    DatasetAccessError::Access(e) => CommitError::Access(e),
                    unexpected_error => CommitError::Internal(unexpected_error.int_err()),
                }
            })?;

        let event = self.validate_event(access_dataset_helper, event).await?;

        let mut new_upstream_ids: Vec<odf::DatasetID> = vec![];
        let mut dependencies_modified = false;

        if let odf::MetadataEvent::SetTransform(transform) = &event {
            dependencies_modified = true;
            for new_input in &transform.inputs {
                if let Some(id) = new_input.dataset_ref.id() {
                    new_upstream_ids.push(id.clone());
                } else {
                    // Normally all references must be resolved to IDs already.
                    // We continue here while expecting MetadataChain to reject
                    // this event.
                }
            }
        }

        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await;

        let commit_result = resolved_dataset
            .commit_event(event, odf::dataset::CommitOpts::default())
            .await?;

        // Note: modify dependencies only after `commit_event` succeeds.
        // TODO: the dependencies should be updated as a part of HEAD change
        if dependencies_modified {
            self.dependency_graph_writer
                .update_dataset_node_dependencies(
                    &self.catalog,
                    &dataset_handle.id,
                    new_upstream_ids,
                )
                .await?;
        }

        Ok(commit_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
