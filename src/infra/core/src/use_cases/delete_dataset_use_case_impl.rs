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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::dataset_pushes::DatasetPush;
use kamu_core::{
    DanglingReferenceError,
    DatasetLifecycleMessage,
    DatasetRepository,
    DeleteDatasetError,
    DeleteDatasetUseCase,
    DependencyGraphService,
    GetDatasetError,
    GetSummaryOpts,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};
use opendatafabric::{DatasetHandle, DatasetRef};

use crate::DatasetRepositoryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DeleteDatasetUseCase)]
pub struct DeleteDatasetUseCaseImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    outbox: Arc<dyn Outbox>,
}

impl DeleteDatasetUseCaseImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_repo_writer,
            dataset_action_authorizer,
            dependency_graph_service,
            outbox,
        }
    }

    async fn ensure_no_dangling_references(
        &self,
        dataset_handle: &DatasetHandle,
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
            let mut children = Vec::with_capacity(downstream_dataset_ids.len());
            for downstream_dataset_id in downstream_dataset_ids {
                let hdl = self
                    .dataset_repo
                    .resolve_dataset_ref(&downstream_dataset_id.as_local_ref())
                    .await
                    .int_err()?;
                children.push(hdl);
            }

            return Err(DanglingReferenceError {
                dataset_handle: dataset_handle.clone(),
                children,
            }
            .into());
        }

        Ok(())
    }

    async fn ensure_no_unsync_remotes(
        &self,
        dataset_handle: &DatasetHandle,
    ) -> Result<(), DeleteDatasetError> {
        let ds = self.dataset_repo.get_dataset_by_handle(dataset_handle);

        match ds.get_summary(GetSummaryOpts::default()).await {
            Ok(summary) => {
                match ds.get_push_info().await {
                    Ok(ds_pushes) => {
                        let head = summary.last_block_hash;
                        let unsync_pushes: Vec<&DatasetPush> = ds_pushes
                            .pushes
                            .iter()
                            .filter(|(_, v)| v.head != head)
                            .map(|(_, v)| v)
                            .collect();
                        //todo
                        if !unsync_pushes.is_empty() {
                            for p in &unsync_pushes {
                                tracing::error!("Out of sync remote: {}", &p.target);
                            }
                            let msg =
                                format!("Out of sync: {}", unsync_pushes.first().unwrap().target);
                            Err(DeleteDatasetError::Internal(msg.int_err()))
                        } else {
                            Ok(())
                        }
                    }
                    Err(e) => Err(DeleteDatasetError::Internal(e.int_err())),
                }
            }
            Err(e) => Err(DeleteDatasetError::Internal(e.int_err())),
        }
    }
}

#[async_trait::async_trait]
impl DeleteDatasetUseCase for DeleteDatasetUseCaseImpl {
    async fn execute_via_ref(&self, dataset_ref: &DatasetRef) -> Result<(), DeleteDatasetError> {
        let dataset_handle = match self.dataset_repo.resolve_dataset_ref(dataset_ref).await {
            Ok(h) => Ok(h),
            Err(GetDatasetError::NotFound(e)) => Err(DeleteDatasetError::NotFound(e)),
            Err(GetDatasetError::Internal(e)) => Err(DeleteDatasetError::Internal(e)),
        }?;

        self.execute_via_handle(&dataset_handle).await
    }

    async fn execute_via_handle(
        &self,
        dataset_handle: &DatasetHandle,
    ) -> Result<(), DeleteDatasetError> {
        // Permission check
        self.dataset_action_authorizer
            .check_action_allowed(dataset_handle, DatasetAction::Write)
            .await?;

        // Validate against dangling ref
        self.ensure_no_dangling_references(dataset_handle).await?;

        self.ensure_no_unsync_remotes(dataset_handle).await?;

        // Do actual delete
        self.dataset_repo_writer
            .delete_dataset(dataset_handle)
            .await?;

        // Notify interested parties
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                DatasetLifecycleMessage::deleted(dataset_handle.id.clone()),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
