// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use {kamu_flow_system as fs, opendatafabric as odf};

use super::{
    check_if_flow_belongs_to_dataset,
    ensure_expected_dataset_kind,
    ensure_scheduling_permission,
    FlowInDatasetError,
    FlowIncompatibleDatasetKind,
    FlowMissingDatasetPollingSource,
    FlowNotFound,
};
use crate::prelude::*;
use crate::queries::Flow;
use crate::utils::ensure_polling_source_exists;
use crate::{utils, LoggedInGuard};

///////////////////////////////////////////////////////////////////////////////

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
    ) -> Result<TriggerFlowResult> {
        if let Some(e) =
            ensure_expected_dataset_kind(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(TriggerFlowResult::IncompatibleDatasetKind(e));
        }

        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;
        if !ensure_polling_source_exists(ctx, &self.dataset_handle).await? {
            return Ok(TriggerFlowResult::MissingDatasetPollingSource(
                FlowMissingDatasetPollingSource,
            ));
        }

        // TODO: for some datasets launching manually might not be an option:
        //   i.e., root datasets with push sources require input data to arrive

        let flow_service = from_catalog::<dyn fs::FlowService>(ctx).unwrap();
        let logged_account = utils::get_logged_account(ctx);

        let flow_state = flow_service
            .trigger_manual_flow(
                Utc::now(),
                fs::FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                odf::AccountID::from(odf::FAKE_ACCOUNT_ID),
                logged_account.account_name,
            )
            .await
            .map_err(|e| match e {
                fs::RequestFlowError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: Flow::new(flow_state),
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
        let flow_service = from_catalog::<dyn fs::FlowService>(ctx).unwrap();
        let flow_state = flow_service
            .cancel_scheduled_tasks(flow_id.into())
            .await
            .map_err(|e| match e {
                fs::CancelScheduledTasksError::NotFound(_) => unreachable!("Flow checked already"),
                fs::CancelScheduledTasksError::Internal(e) => GqlError::Internal(e),
            })?;

        // Pause flow configuration regardless of current state.
        // Duplicate requests are auto-ignored.
        let flow_configuration_service =
            from_catalog::<dyn fs::FlowConfigurationService>(ctx).unwrap();
        flow_configuration_service
            .pause_flow_configuration(Utc::now(), flow_state.flow_key.clone())
            .await?;

        Ok(CancelScheduledTasksResult::Success(
            CancelScheduledTasksSuccess {
                flow: Flow::new(flow_state),
            },
        ))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum TriggerFlowResult {
    Success(TriggerFlowSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    MissingDatasetPollingSource(FlowMissingDatasetPollingSource),
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

///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////
