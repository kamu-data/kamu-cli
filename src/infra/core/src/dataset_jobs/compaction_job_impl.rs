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
    CompactionError,
    CompactionExecutor,
    CompactionJob,
    CompactionListener,
    CompactionOptions,
    CompactionPlanner,
    CompactionResult,
    NullCompactionListener,
};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetReferenceUpdater,
    DatasetRegistry,
};

use super::job_target::{authorize_and_resolve_job_target, commit_job_head};
use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CompactionJob)]
pub struct CompactionJobImpl {
    catalog: dill::Catalog,
    compaction_executor: Arc<dyn CompactionExecutor>,
    principal: Arc<Principal>,
}

impl CompactionJobImpl {
    async fn compact_and_commit(
        &self,
        target_handle: &odf::DatasetHandle,
        options: CompactionOptions,
        listener: Arc<dyn CompactionListener>,
    ) -> Result<CompactionResult, CompactionError> {
        // 1: Authorize and plan
        let (target, plan) =
            transaction!(|dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
                          dataset_registry: Arc<dyn DatasetRegistry>,
                          planner: Arc<dyn CompactionPlanner>| {
                let target = authorize_and_resolve_job_target::<CompactionError>(
                    dataset_operation_guard.as_ref(),
                    dataset_registry.as_ref(),
                    &self.principal,
                    target_handle,
                    Self::operation(&options),
                )
                .await?;
                let plan = planner
                    .plan_compaction(target.clone(), options, Some(listener.clone()))
                    .await?;
                target.detach_from_transaction();

                (target, plan)
            });

        // 2: Merge, with no transaction open
        let result = self
            .compaction_executor
            .execute(target, plan, Some(listener))
            .await?;

        // 3: Commit, if something got compacted
        if let CompactionResult::Success {
            old_head, new_head, ..
        } = &result
        {
            transaction!(|dataset_registry: Arc<dyn DatasetRegistry>,
                          updater: Arc<dyn DatasetReferenceUpdater>| {
                commit_job_head::<CompactionError>(
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

    /// Compacting to metadata only drops all data, so it needs the right to
    /// reset rather than to compact
    fn operation(options: &CompactionOptions) -> DatasetOperation {
        if options.keep_metadata_only {
            DatasetOperation::ResetToMetadata
        } else {
            DatasetOperation::Compaction
        }
    }

    fn span_outcome(result: &Result<CompactionResult, CompactionError>) -> SpanOutcome {
        match result {
            Ok(CompactionResult::Success { .. }) => SpanOutcome::Updated,
            Ok(CompactionResult::NothingToDo) => SpanOutcome::UpToDate,
            Err(CompactionError::NotFound(_)) => SpanOutcome::DatasetDeleted,
            Err(CompactionError::RefCASFailed(_)) => SpanOutcome::RefMoved,
            Err(CompactionError::Access(_)) => SpanOutcome::NotAllowed,
            Err(
                CompactionError::Planning(_)
                | CompactionError::Execution(_)
                | CompactionError::Internal(_),
            ) => SpanOutcome::Failed,
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CompactionJob for CompactionJobImpl {
    #[tracing::instrument(level = "info", name = CompactionJobImpl_run, skip_all, fields(target = %target, ?options, outcome))]
    async fn run(
        &self,
        target: &odf::DatasetHandle,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullCompactionListener {}));
        listener.begin();

        let result = self
            .compact_and_commit(target, options, listener.clone())
            .await;

        match &result {
            Ok(compaction_result) => listener.success(compaction_result),
            Err(e) => listener.error(e),
        }
        Self::span_outcome(&result).record();
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
