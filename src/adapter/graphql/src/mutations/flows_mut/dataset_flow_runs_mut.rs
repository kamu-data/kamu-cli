// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_flow_system as fs;

use super::{
    check_if_flow_belongs_to_dataset,
    ensure_expected_dataset_kind,
    ensure_flow_preconditions,
    ensure_scheduling_permission,
    FlowInDatasetError,
    FlowIncompatibleDatasetKind,
    FlowNotFound,
    FlowPreconditionsNotMet,
};
use crate::prelude::*;
use crate::queries::Flow;
use crate::{utils, LoggedInGuard};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowRunsMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetFlowRunsMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn trigger_flow(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        flow_run_configuration: Option<FlowRunConfiguration>,
    ) -> Result<TriggerFlowResult> {
        if let Some(e) = ensure_expected_dataset_kind(
            ctx,
            &self.dataset_handle,
            dataset_flow_type,
            flow_run_configuration.as_ref(),
        )
        .await?
        {
            return Ok(TriggerFlowResult::IncompatibleDatasetKind(e));
        }

        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;

        if let Some(e) = ensure_flow_preconditions(
            ctx,
            &self.dataset_handle,
            dataset_flow_type,
            flow_run_configuration.as_ref(),
        )
        .await?
        {
            return Ok(TriggerFlowResult::PreconditionsNotMet(e));
        }

        // TODO: for some datasets launching manually might not be an option:
        //   i.e., root datasets with push sources require input data to arrive

        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);
        let logged_account = utils::get_logged_account(ctx)?;

        let flow_run_snapshot = match FlowRunConfiguration::try_into_snapshot(
            ctx,
            &dataset_flow_type,
            &self.dataset_handle,
            flow_run_configuration.as_ref(),
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(err) => return Ok(TriggerFlowResult::InvalidRunConfigurations(err)),
        };

        let flow_state = flow_query_service
            .trigger_manual_flow(
                Utc::now(),
                fs::FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                logged_account.account_id,
                flow_run_snapshot,
            )
            .await
            .map_err(|e| match e {
                fs::RequestFlowError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn cancel_scheduled_tasks(
        &self,
        ctx: &Context<'_>,
        flow_id: FlowID,
    ) -> Result<CancelScheduledTasksResult> {
        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;

        if let Some(error) =
            check_if_flow_belongs_to_dataset(ctx, flow_id, &self.dataset_handle).await?
        {
            return Ok(match error {
                FlowInDatasetError::NotFound(e) => CancelScheduledTasksResult::NotFound(e),
            });
        }

        // Attempt cancelling scheduled tasks
        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);
        let flow_state = flow_query_service
            .cancel_scheduled_tasks(flow_id.into())
            .await
            .map_err(|e| match e {
                fs::CancelScheduledTasksError::NotFound(_) => unreachable!("Flow checked already"),
                fs::CancelScheduledTasksError::Internal(e) => GqlError::Internal(e),
            })?;

        // Pause flow triggers regardless of current state.
        // Duplicate requests are auto-ignored.
        let flow_trigger_service = from_catalog_n!(ctx, dyn fs::FlowTriggerService);
        flow_trigger_service
            .pause_flow_trigger(Utc::now(), flow_state.flow_key.clone())
            .await?;

        Ok(CancelScheduledTasksResult::Success(
            CancelScheduledTasksSuccess {
                flow: Flow::build_batch(vec![flow_state], ctx)
                    .await?
                    .pop()
                    .unwrap(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum TriggerFlowResult {
    Success(TriggerFlowSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    PreconditionsNotMet(FlowPreconditionsNotMet),
    InvalidRunConfigurations(FlowInvalidRunConfigurations),
}

#[derive(SimpleObject)]
#[graphql(complex)]
struct TriggerFlowSuccess {
    pub flow: Flow,
}

#[ComplexObject]
impl TriggerFlowSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum CancelScheduledTasksResult {
    Success(CancelScheduledTasksSuccess),
    NotFound(FlowNotFound),
}

#[derive(SimpleObject)]
#[graphql(complex)]
struct CancelScheduledTasksSuccess {
    pub flow: Flow,
}

#[ComplexObject]
impl CancelScheduledTasksSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct FlowInvalidRunConfigurations {
    pub error: String,
}

#[ComplexObject]
impl FlowInvalidRunConfigurations {
    pub async fn message(&self) -> String {
        format!("Invalid flow configuration provided: '{}'", self.error)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
