// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dill::{component, interface};
use internal_error::ResultIntoInternal;
use kamu_datasets::*;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

use crate::DatasetEntryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DeleteDatasetUseCase)]
pub struct DeleteDatasetUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    time_source: Arc<dyn SystemTimeSource>,
    outbox: Arc<dyn Outbox>,
}

impl DeleteDatasetUseCaseImpl {
    async fn delete_storage_unit_and_notify(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<(), DeleteDatasetError> {
        match self
            .dataset_storage_unit_writer
            .delete_dataset(&dataset_handle.id)
            .await
        {
            Ok(()) => {}
            Err(odf::dataset::DeleteStoredDatasetError::UnresolvedId(_)) => {
                tracing::warn!(
                    dataset_id = %dataset_handle.id,
                    "Dataset storage unit is already absent. Continuing idempotent deletion"
                );
            }
            Err(odf::dataset::DeleteStoredDatasetError::Internal(e)) => return Err(e.into()),
        }

        // Notify interested parties
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::deleted(self.time_source.now(), dataset_handle.id.clone()),
            )
            .await?;

        Ok(())
    }

    async fn resolve_delete_candidate_handles(
        &self,
        seed_dataset_handles: Vec<odf::DatasetHandle>,
        recursive: bool,
    ) -> Result<Vec<odf::DatasetHandle>, DeleteDatasetPlanningError> {
        if !recursive {
            return Ok(seed_dataset_handles);
        }

        use tokio_stream::StreamExt;

        let dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_recursive_downstream_dependencies(
                seed_dataset_handles
                    .into_iter()
                    .map(|dataset_handle| dataset_handle.id)
                    .collect(),
            )
            .await
            .int_err()?
            .map(Cow::Owned)
            .collect::<Vec<_>>()
            .await;

        let resolution = self
            .dataset_registry
            .resolve_multiple_dataset_handles_by_ids(&dataset_ids)
            .await
            .int_err()?;

        if let Some((_, error)) = resolution.unresolved_datasets.into_iter().next() {
            return Err(match error {
                odf::DatasetRefUnresolvedError::NotFound(e) => e.into(),
                odf::DatasetRefUnresolvedError::Internal(e) => e.into(),
            });
        }

        Ok(resolution.resolved_handles)
    }

    async fn resolve_direct_downstream_handles(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<Vec<odf::DatasetHandle>, DeleteDatasetPlanningError> {
        use tokio_stream::StreamExt;

        let downstream_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_downstream_dependencies(&dataset_handle.id)
            .await
            .collect::<Vec<_>>()
            .await;

        let mut downstream_handles = Vec::with_capacity(downstream_dataset_ids.len());

        for downstream_dataset_id in downstream_dataset_ids {
            // Intentionally checking downstream datasets without considering dataset
            // visibility.
            match self
                .dataset_registry
                .resolve_dataset_handle_by_ref(&downstream_dataset_id.as_local_ref())
                .await
            {
                Ok(hdl) => downstream_handles.push(hdl),
                Err(odf::dataset::DatasetRefUnresolvedError::NotFound(_)) => {
                    tracing::warn!(
                        "Skipped unresolved downstream reference: {downstream_dataset_id}"
                    );
                    // Skip this not found error, as the dependency graph
                    // in-memory is only updated at the end
                    // of the current transaction, and in case
                    // of recursive or all delete modes, we might have already
                    // deleted this dataset.
                }
                Err(odf::dataset::DatasetRefUnresolvedError::Internal(e)) => {
                    return Err(e.into());
                }
            }
        }

        Ok(downstream_handles)
    }

    async fn collect_dangling_references(
        &self,
        delete_targets: &[DeleteDatasetPlanTarget],
        unauthorized_recursive_dataset_ids: &HashSet<odf::DatasetID>,
    ) -> Result<Vec<DanglingReferenceError>, DeleteDatasetPlanningError> {
        let target_dataset_ids: HashSet<_> = delete_targets
            .iter()
            .map(|target| target.dataset_handle.id.clone())
            .collect();
        let mut dangling_references = Vec::new();

        for target in delete_targets {
            let dangling_children = self
                .resolve_direct_downstream_handles(&target.dataset_handle)
                .await?
                .into_iter()
                .filter(|downstream_handle| !target_dataset_ids.contains(&downstream_handle.id))
                .filter(|downstream_handle| {
                    !unauthorized_recursive_dataset_ids.contains(&downstream_handle.id)
                })
                .collect::<Vec<_>>();

            if !dangling_children.is_empty() {
                dangling_references.push(DanglingReferenceError {
                    dataset_handle: target.dataset_handle.clone(),
                    children: dangling_children,
                });
            }
        }

        Ok(dangling_references)
    }

    async fn order_authorized_delete_targets(
        &self,
        authorized_handles: Vec<odf::DatasetHandle>,
    ) -> Result<Vec<DeleteDatasetPlanTarget>, DeleteDatasetPlanningError> {
        let dataset_ids = self
            .dependency_graph_service
            .in_dependency_order(
                authorized_handles
                    .iter()
                    .map(|dataset_handle| dataset_handle.id.clone())
                    .collect(),
                DependencyOrder::DepthFirst,
            )
            .await
            .int_err()?;

        let mut handles_by_id: HashMap<_, _> = authorized_handles
            .into_iter()
            .map(|dataset_handle| (dataset_handle.id.clone(), dataset_handle))
            .collect();

        Ok(dataset_ids
            .into_iter()
            .map(|dataset_id| DeleteDatasetPlanTarget {
                dataset_handle: handles_by_id
                    .remove(&dataset_id)
                    .expect("Dependency ordering must return only authorized dataset IDs"),
            })
            .collect())
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl DeleteDatasetUseCase for DeleteDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = DeleteDatasetUseCaseImpl_plan_delete,
        skip_all,
        fields(recursive)
    )]
    async fn plan_delete(
        &self,
        seed_dataset_handles: Vec<odf::DatasetHandle>,
        recursive: bool,
    ) -> Result<DeleteDatasetPlanningResult, DeleteDatasetPlanningError> {
        let selected_dataset_ids: HashSet<_> = seed_dataset_handles
            .iter()
            .map(|dataset_handle| dataset_handle.id.clone())
            .collect();

        let candidate_handles = self
            .resolve_delete_candidate_handles(seed_dataset_handles, recursive)
            .await?;

        let classification = self
            .dataset_action_authorizer
            .classify_dataset_handles_by_allowance(candidate_handles, DatasetAction::Own)
            .await?;

        let (unauthorized_selected_handles, unauthorized_recursive_handles): (Vec<_>, Vec<_>) =
            classification
                .unauthorized_handles_with_errors
                .into_iter()
                .partition(|(dataset_handle, _)| selected_dataset_ids.contains(&dataset_handle.id));

        let authorized_targets = self
            .order_authorized_delete_targets(classification.authorized_handles)
            .await?;

        let unauthorized_recursive_dataset_ids = unauthorized_recursive_handles
            .iter()
            .map(|(dataset_handle, _)| dataset_handle.id.clone())
            .collect();

        let dangling_references = self
            .collect_dangling_references(&authorized_targets, &unauthorized_recursive_dataset_ids)
            .await?;

        Ok(DeleteDatasetPlanningResult {
            plan: DeleteDatasetPlan { authorized_targets },
            issues: DeleteDatasetPlanIssues {
                unauthorized_selected_handles,
                unauthorized_recursive_handles,
                dangling_references,
            },
        })
    }

    #[tracing::instrument(
        level = "info",
        name = DeleteDatasetUseCaseImpl_execute_plan,
        skip_all
    )]
    async fn execute_plan(
        &self,
        plan: DeleteDatasetPlan,
    ) -> Result<DeleteDatasetExecutionSummary, DeleteDatasetError> {
        let summary = DeleteDatasetExecutionSummary {
            deleted_dataset_handles: plan
                .authorized_targets
                .into_iter()
                .map(|target| target.dataset_handle)
                .collect(),
        };

        self.dataset_entry_writer
            .remove_entries(&summary.deleted_dataset_handles)
            .await?;

        for dataset_handle in &summary.deleted_dataset_handles {
            self.delete_storage_unit_and_notify(dataset_handle).await?;
        }

        Ok(summary)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
