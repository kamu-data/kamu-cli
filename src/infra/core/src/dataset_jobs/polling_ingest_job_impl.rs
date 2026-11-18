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
    DataWriterMetadataState,
    PollingIngestJob,
    PollingIngestJobError,
    PollingIngestListener,
    PollingIngestOptions,
    PollingIngestPlan,
    PollingIngestPlanError,
    PollingIngestPlanner,
    PollingIngestResult,
    PollingIngestService,
};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetReferenceUpdater,
    DatasetRegistry,
};
use random_strings::get_random_name;

use super::job_target::{authorize_and_resolve_job_target, commit_job_head};
use crate::IngestEnvVarsResolver;
use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn PollingIngestJob)]
pub struct PollingIngestJobImpl {
    catalog: dill::Catalog,
    polling_ingest_service: Arc<dyn PollingIngestService>,
    principal: Arc<Principal>,
}

impl PollingIngestJobImpl {
    async fn ingest_and_commit(
        &self,
        target_handle: &odf::DatasetHandle,
        options: PollingIngestOptions,
        maybe_continuation: Option<Box<DataWriterMetadataState>>,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PollingIngestJobError> {
        // 1: Authorize and plan
        let (target, plan, env_vars) =
            transaction!(|dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
                          dataset_registry: Arc<dyn DatasetRegistry>,
                          planner: Arc<dyn PollingIngestPlanner>,
                          env_vars_resolver: Arc<IngestEnvVarsResolver>| {
                let target = authorize_and_resolve_job_target::<PollingIngestJobError>(
                    dataset_operation_guard.as_ref(),
                    dataset_registry.as_ref(),
                    &self.principal,
                    target_handle,
                    DatasetOperation::PollingIngest,
                )
                .await?;
                let plan = match maybe_continuation {
                    Some(metadata_state) => PollingIngestPlan::new(target_handle, metadata_state)
                        .map_err(PollingIngestPlanError::from)?,
                    None => planner.plan_ingest(target.clone()).await?,
                };
                let env_vars = env_vars_resolver
                    .resolve(&target_handle.id)
                    .await
                    .map_err(PollingIngestPlanError::Internal)?;
                target.detach_from_transaction();

                (target, plan, env_vars)
            });

        // 2: Fetch, read and stage, with no transaction open
        let result = self
            .polling_ingest_service
            .ingest(
                target,
                plan,
                options,
                &env_vars,
                &get_random_name(None, 10),
                maybe_listener,
            )
            .await?;

        // 3: Commit, if the iteration appended blocks
        if let PollingIngestResult::Updated {
            old_head, new_head, ..
        } = &result
        {
            transaction!(|dataset_registry: Arc<dyn DatasetRegistry>,
                          updater: Arc<dyn DatasetReferenceUpdater>| {
                commit_job_head::<PollingIngestJobError>(
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

    fn span_outcome(result: &Result<PollingIngestResult, PollingIngestJobError>) -> SpanOutcome {
        match result {
            Ok(PollingIngestResult::Updated { .. }) => SpanOutcome::Updated,
            Ok(PollingIngestResult::UpToDate { .. }) => SpanOutcome::UpToDate,
            Err(PollingIngestJobError::NotFound(_)) => SpanOutcome::DatasetDeleted,
            Err(PollingIngestJobError::RefCASFailed(_)) => SpanOutcome::RefMoved,
            Err(PollingIngestJobError::Access(_)) => SpanOutcome::NotAllowed,
            Err(
                PollingIngestJobError::Planning(_)
                | PollingIngestJobError::Ingest(_)
                | PollingIngestJobError::Internal(_),
            ) => SpanOutcome::Failed,
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl PollingIngestJob for PollingIngestJobImpl {
    #[tracing::instrument(level = "info", name = PollingIngestJobImpl_run, skip_all, fields(target = %target, outcome))]
    async fn run(
        &self,
        target: &odf::DatasetHandle,
        options: PollingIngestOptions,
        maybe_continuation: Option<Box<DataWriterMetadataState>>,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PollingIngestJobError> {
        let result = self
            .ingest_and_commit(target, options, maybe_continuation, maybe_listener)
            .await;
        Self::span_outcome(&result).record();
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
