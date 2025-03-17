// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_flow_system::{FlowKeyDataset, FlowTriggerRule, FlowTriggerService};

use super::{
    ensure_expected_dataset_kind,
    ensure_flow_preconditions,
    ensure_scheduling_permission,
    FlowIncompatibleDatasetKind,
    FlowPreconditionsNotMet,
    FlowTypeIsNotSupported,
};
use crate::prelude::*;
use crate::queries::DatasetRequestState;
use crate::LoggedInGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowTriggersMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetFlowTriggersMut<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    #[tracing::instrument(level = "info", name = DatasetFlowTriggersMut_set_trigger, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_trigger(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        paused: bool,
        trigger_input: FlowTriggerInput,
    ) -> Result<SetFlowTriggerResult> {
        ensure_scheduling_permission(ctx, self.dataset_request_state).await?;

        if let Err(err) = trigger_input.check_type_compatible(dataset_flow_type) {
            return Ok(SetFlowTriggerResult::TypeIsNotSupported(err));
        };

        if let Some(e) =
            ensure_expected_dataset_kind(ctx, self.dataset_request_state, dataset_flow_type, None)
                .await?
        {
            return Ok(SetFlowTriggerResult::IncompatibleDatasetKind(e));
        }

        let trigger_rule: FlowTriggerRule = match trigger_input.try_into() {
            Ok(rule) => rule,
            Err(e) => return Ok(SetFlowTriggerResult::FlowInvalidTriggerInput(e)),
        };

        if let Some(e) =
            ensure_flow_preconditions(ctx, self.dataset_request_state, dataset_flow_type, None)
                .await?
        {
            return Ok(SetFlowTriggerResult::PreconditionsNotMet(e));
        }

        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);
        let dataset_handle = self.dataset_request_state.dataset_handle();

        let res = flow_trigger_service
            .set_trigger(
                Utc::now(),
                FlowKeyDataset::new(dataset_handle.id.clone(), dataset_flow_type.into()).into(),
                paused,
                trigger_rule,
            )
            .await
            .int_err()?;

        Ok(SetFlowTriggerResult::Success(SetFlowTriggerSuccess {
            trigger: res.into(),
        }))
    }

    #[tracing::instrument(level = "info", name = DatasetFlowTriggersMut_pause_flows, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn pause_flows(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<bool> {
        ensure_scheduling_permission(ctx, self.dataset_request_state).await?;

        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

        let dataset_handle = self.dataset_request_state.dataset_handle();

        flow_trigger_service
            .pause_dataset_flows(
                Utc::now(),
                &dataset_handle.id,
                dataset_flow_type.map(Into::into),
            )
            .await?;

        Ok(true)
    }

    #[tracing::instrument(level = "info", name = DatasetFlowTriggersMut_resume_flows, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resume_flows(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<bool> {
        ensure_scheduling_permission(ctx, self.dataset_request_state).await?;

        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

        let dataset_handle = self.dataset_request_state.dataset_handle();

        flow_trigger_service
            .resume_dataset_flows(
                Utc::now(),
                &dataset_handle.id,
                dataset_flow_type.map(Into::into),
            )
            .await?;

        Ok(true)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum SetFlowTriggerResult {
    Success(SetFlowTriggerSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    PreconditionsNotMet(FlowPreconditionsNotMet),
    TypeIsNotSupported(FlowTypeIsNotSupported),
    FlowInvalidTriggerInput(FlowInvalidTriggerInputError),
}

#[derive(SimpleObject)]
#[graphql(complex)]
struct SetFlowTriggerSuccess {
    pub trigger: FlowTrigger,
}

#[ComplexObject]
impl SetFlowTriggerSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct FlowInvalidTriggerInputError {
    pub reason: String,
}

#[ComplexObject]
impl FlowInvalidTriggerInputError {
    pub async fn message(&self) -> String {
        self.reason.clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
