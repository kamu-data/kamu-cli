// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::transaction;
use dill::{component, interface};
use kamu_accounts::Principal;
use kamu_core::{
    TransformElaboration,
    TransformExecutor,
    TransformJob,
    TransformJobError,
    TransformListener,
    TransformPlanner,
    TransformResult,
};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetReferenceUpdater,
    DatasetRegistry,
};
use random_strings::get_random_name;

use super::job_target::{commit_job_head, resolve_job_target};
use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TransformJob)]
pub struct TransformJobImpl {
    catalog: dill::Catalog,
    transform_executor: Arc<dyn TransformExecutor>,
    principal: Arc<Principal>,
}

impl TransformJobImpl {
    async fn elaborate_execute_and_commit(
        &self,
        target_handle: &odf::DatasetHandle,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformJobError> {
        // 1: Authorize and elaborate
        let elaborated =
            transaction!(|dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
                          dataset_registry: Arc<dyn DatasetRegistry>,
                          planner: Arc<dyn TransformPlanner>| {
                let target = resolve_job_target::<TransformJobError>(
                    dataset_registry.as_ref(),
                    target_handle,
                )
                .await?;

                // Access covers exactly what the elaboration read, and is checked
                // before any of its results is used
                let elaboration = planner.elaborate_transform(target.clone()).await;
                let inputs = match &elaboration {
                    Ok(TransformElaboration::Elaborated(plan)) => plan
                        .request
                        .inputs
                        .iter()
                        .map(|input| input.dataset_handle.clone())
                        .collect(),
                    Ok(TransformElaboration::UpToDate) | Err(_) => Vec::new(),
                };
                dataset_operation_guard
                    .check_operation(
                        &self.principal,
                        &DatasetOperationRequest::new(
                            target_handle.clone(),
                            DatasetOperation::Transform { inputs },
                        ),
                    )
                    .await?;
                let elaboration = elaboration?;

                target.detach_from_transaction();
                match elaboration {
                    TransformElaboration::Elaborated(plan) => {
                        plan.datasets_map.detach_from_transaction();
                        Some((target, plan))
                    }
                    TransformElaboration::UpToDate => None,
                }
            });

        let Some((target, plan)) = elaborated else {
            return Ok(TransformResult::UpToDate);
        };

        // 2: Run the engine, with no transaction open
        let (_, result) = self
            .transform_executor
            .execute_transform(target, plan, &get_random_name(None, 10), maybe_listener)
            .await;
        let result = result?;

        // 3: Commit, if the engine appended blocks
        if let TransformResult::Updated {
            old_head, new_head, ..
        } = &result
        {
            transaction!(|dataset_registry: Arc<dyn DatasetRegistry>,
                          updater: Arc<dyn DatasetReferenceUpdater>| {
                commit_job_head::<TransformJobError>(
                    dataset_registry.as_ref(),
                    updater.as_ref(),
                    target_handle,
                    Some(old_head),
                    new_head,
                )
                .await?;
            });
        }

        Ok(result)
    }

    fn span_outcome(result: &Result<TransformResult, TransformJobError>) -> SpanOutcome {
        match result {
            Ok(TransformResult::Updated { .. }) => SpanOutcome::Updated,
            Ok(TransformResult::UpToDate) => SpanOutcome::UpToDate,
            Err(TransformJobError::NotFound(_)) => SpanOutcome::DatasetDeleted,
            Err(TransformJobError::RefMoved(_)) => SpanOutcome::RefMoved,
            Err(TransformJobError::Access(_)) => SpanOutcome::NotAllowed,
            Err(
                TransformJobError::Elaborate(_)
                | TransformJobError::Execute(_)
                | TransformJobError::Internal(_),
            ) => SpanOutcome::Failed,
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl TransformJob for TransformJobImpl {
    #[tracing::instrument(level = "info", name = TransformJobImpl_run, skip_all, fields(target = %target, outcome))]
    async fn run(
        &self,
        target: &odf::DatasetHandle,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformJobError> {
        let result = self
            .elaborate_execute_and_commit(target, maybe_listener)
            .await;
        Self::span_outcome(&result).record();
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
