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
    ResetError,
    ResetExecutionError,
    ResetExecutor,
    ResetJob,
    ResetPlanner,
    ResetResult,
};
use kamu_datasets::{DatasetOperation, DatasetOperationGuard, DatasetRegistry};

use super::job_target::authorize_and_resolve_job_target;
use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ResetJob)]
pub struct ResetJobImpl {
    catalog: dill::Catalog,
    principal: Arc<Principal>,
}

impl ResetJobImpl {
    fn span_outcome(result: &Result<ResetResult, ResetError>) -> SpanOutcome {
        use odf::dataset::SetChainRefError as E;

        match result {
            Ok(_) => SpanOutcome::Updated,
            Err(ResetError::NotFound(_)) => SpanOutcome::DatasetDeleted,
            Err(ResetError::Execution(ResetExecutionError::SetReferenceFailed(E::CASFailed(
                _,
            )))) => SpanOutcome::RefMoved,
            Err(
                ResetError::Access(_)
                | ResetError::Execution(ResetExecutionError::SetReferenceFailed(E::Access(_))),
            ) => SpanOutcome::NotAllowed,
            Err(
                ResetError::Planning(_)
                | ResetError::Execution(ResetExecutionError::SetReferenceFailed(
                    E::BlockNotFound(_) | E::Internal(_),
                ))
                | ResetError::Internal(_),
            ) => SpanOutcome::Failed,
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl ResetJob for ResetJobImpl {
    #[tracing::instrument(level = "info", name = ResetJobImpl_run, skip_all, fields(%target, ?maybe_new_head, ?maybe_old_head, outcome))]
    async fn run(
        &self,
        target: &odf::DatasetHandle,
        maybe_new_head: Option<&odf::Multihash>,
        maybe_old_head: Option<&odf::Multihash>,
    ) -> Result<ResetResult, ResetError> {
        // The block to reset to already exists, so planning and moving `HEAD`
        // share one short transaction
        let result = transaction!(|dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
                                   dataset_registry: Arc<dyn DatasetRegistry>,
                                   planner: Arc<dyn ResetPlanner>,
                                   executor: Arc<dyn ResetExecutor>|
         -> Result<ResetResult, ResetError> {
            let dataset = authorize_and_resolve_job_target::<ResetError>(
                dataset_operation_guard.as_ref(),
                dataset_registry.as_ref(),
                &self.principal,
                target,
                DatasetOperation::Reset,
            )
            .await?;
            let plan = planner
                .plan_reset(dataset.clone(), maybe_new_head, maybe_old_head)
                .await?;
            Ok(executor.execute(dataset, plan).await?)
        });

        Self::span_outcome(&result).record();
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
