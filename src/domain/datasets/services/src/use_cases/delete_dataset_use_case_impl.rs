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
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::{DatasetRegistry, DependencyGraphService};
use kamu_datasets::{
    DanglingReferenceError,
    DatasetLifecycleMessage,
    DeleteDatasetError,
    DeleteDatasetUseCase,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};

use crate::DatasetEntryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DeleteDatasetUseCase)]
pub struct DeleteDatasetUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    outbox: Arc<dyn Outbox>,
}

impl DeleteDatasetUseCaseImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
        dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_entry_writer,
            dataset_storage_unit_writer,
            dataset_action_authorizer,
            dependency_graph_service,
            outbox,
        }
    }

    async fn ensure_no_dangling_references(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<(), DeleteDatasetError> {
        use tokio_stream::StreamExt;
        let downstream_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_downstream_dependencies(&dataset_handle.id)
            .await
            .int_err()?
            .collect()
            .await;

        if !downstream_dataset_ids.is_empty() {
            let mut dangling_children = Vec::with_capacity(downstream_dataset_ids.len());
            for downstream_dataset_id in downstream_dataset_ids {
                match self
                    .dataset_registry
                    .resolve_dataset_handle_by_ref(&downstream_dataset_id.as_local_ref())
                    .await
                {
                    Ok(hdl) => dangling_children.push(hdl),
                    Err(odf::dataset::DatasetRefUnresolvedError::NotFound(_)) => {
                        tracing::warn!(
                            "Skipped unresolved downstream reference: {downstream_dataset_id}"
                        );
                        // Skip this not found error, as the dependency graph
                        // in-memory is only updated at the end of the current
                        // transaction, and in case of recursive or all delete
                        // modes, we might have already deleted this dataset
                    }
                    Err(odf::dataset::DatasetRefUnresolvedError::Internal(e)) => {
                        return Err(e.into())
                    }
                };
            }

            if !dangling_children.is_empty() {
                return Err(DanglingReferenceError {
                    dataset_handle: dataset_handle.clone(),
                    children: dangling_children,
                }
                .into());
            }
        }

        Ok(())
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl DeleteDatasetUseCase for DeleteDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = DeleteDatasetUseCaseImpl_execute_via_ref,
        skip_all,
        fields(dataset_ref)
    )]
    async fn execute_via_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<(), DeleteDatasetError> {
        let dataset_handle = match self
            .dataset_registry
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await
        {
            Ok(h) => Ok(h),
            Err(odf::DatasetRefUnresolvedError::NotFound(e)) => {
                Err(DeleteDatasetError::NotFound(e))
            }
            Err(odf::DatasetRefUnresolvedError::Internal(e)) => {
                Err(DeleteDatasetError::Internal(e))
            }
        }?;

        self.execute_via_handle(&dataset_handle).await
    }

    #[tracing::instrument(
        level = "info",
        name = DeleteDatasetUseCaseImpl_execute_via_handle,
        skip_all,
        fields(dataset_handle)
    )]
    async fn execute_via_handle(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<(), DeleteDatasetError> {
        // Permission check
        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle.id, DatasetAction::Own)
            .await
            .map_err(|e| match e {
                DatasetActionUnauthorizedError::Access(e) => DeleteDatasetError::Access(e),
                DatasetActionUnauthorizedError::Internal(e) => DeleteDatasetError::Internal(e),
            })?;

        // Validate against dangling ref
        self.ensure_no_dangling_references(dataset_handle).await?;

        // Remove entry
        self.dataset_entry_writer
            .remove_entry(dataset_handle)
            .await?;

        // Do actual delete
        self.dataset_storage_unit_writer
            .delete_dataset(&dataset_handle.id)
            .await?;

        // Notify interested parties
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::deleted(dataset_handle.id.clone()),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
