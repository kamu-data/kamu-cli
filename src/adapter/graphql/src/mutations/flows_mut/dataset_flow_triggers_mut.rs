// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_adapter_flow_dataset::FlowScopeDataset;
use kamu_flow_system::{FlowBinding, FlowTriggerRule, FlowTriggerService, FlowTriggerStopPolicy};

use super::{
    FlowIncompatibleDatasetKind,
    FlowPreconditionsNotMet,
    FlowTypeIsNotSupported,
    ensure_expected_dataset_kind,
    ensure_flow_preconditions,
};
use crate::LoggedInGuard;
use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowTriggersMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
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
    pub async fn set_trigger(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        trigger_rule_input: FlowTriggerRuleInput,
        trigger_stop_policy_input: FlowTriggerStopPolicyInput,
    ) -> Result<SetFlowTriggerResult> {
        if let Err(err) = trigger_rule_input.check_type_compatible(dataset_flow_type) {
            return Ok(SetFlowTriggerResult::TypeIsNotSupported(err));
        }

        if let Some(e) =
            ensure_expected_dataset_kind(ctx, self.dataset_request_state, dataset_flow_type).await?
        {
            return Ok(SetFlowTriggerResult::IncompatibleDatasetKind(e));
        }

        let trigger_rule: FlowTriggerRule = match trigger_rule_input.try_into() {
            Ok(rule) => rule,
            Err(e) => return Ok(SetFlowTriggerResult::InvalidTriggerInput(e)),
        };

        let triggest_stop_policy: FlowTriggerStopPolicy = match trigger_stop_policy_input.try_into()
        {
            Ok(policy) => policy,
            Err(e) => return Ok(SetFlowTriggerResult::InvalidTriggerStopPolicyInput(e)),
        };

        if let Some(e) =
            ensure_flow_preconditions(ctx, self.dataset_request_state, dataset_flow_type, None)
                .await?
        {
            return Ok(SetFlowTriggerResult::PreconditionsNotMet(e));
        }

        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

        let flow_binding = FlowBinding::new(
            encode_dataset_flow_type(dataset_flow_type),
            FlowScopeDataset::make_scope(self.dataset_request_state.dataset_id()),
        );

        let res = flow_trigger_service
            .set_trigger(Utc::now(), flow_binding, trigger_rule, triggest_stop_policy)
            .await
            .int_err()?;

        Ok(SetFlowTriggerResult::Success(SetFlowTriggerSuccess {
            trigger: res.into(),
        }))
    }

    #[tracing::instrument(level = "info", name = DatasetFlowTriggersMut_pause_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn pause_flow(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
    ) -> Result<bool> {
        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

        let flow_binding = FlowBinding::new(
            encode_dataset_flow_type(dataset_flow_type),
            FlowScopeDataset::make_scope(self.dataset_request_state.dataset_id()),
        );

        flow_trigger_service
            .pause_flow_trigger(Utc::now(), &flow_binding)
            .await?;

        Ok(true)
    }

    #[tracing::instrument(level = "info", name = DatasetFlowTriggersMut_pause_flows, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn pause_flows(&self, ctx: &Context<'_>) -> Result<bool> {
        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

        let flow_scope = FlowScopeDataset::make_scope(self.dataset_request_state.dataset_id());
        flow_trigger_service
            .pause_flow_triggers_for_scopes(Utc::now(), &[flow_scope])
            .await?;

        Ok(true)
    }

    #[tracing::instrument(level = "info", name = DatasetFlowTriggersMut_resume_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn resume_flow(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
    ) -> Result<bool> {
        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

        let flow_binding = FlowBinding::new(
            encode_dataset_flow_type(dataset_flow_type),
            FlowScopeDataset::make_scope(self.dataset_request_state.dataset_id()),
        );

        flow_trigger_service
            .resume_flow_trigger(Utc::now(), &flow_binding)
            .await?;

        Ok(true)
    }

    #[tracing::instrument(level = "info", name = DatasetFlowTriggersMut_resume_flows, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn resume_flows(&self, ctx: &Context<'_>) -> Result<bool> {
        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

        let flow_scope = FlowScopeDataset::make_scope(self.dataset_request_state.dataset_id());
        flow_trigger_service
            .resume_flow_triggers_for_scopes(Utc::now(), &[flow_scope])
            .await?;

        Ok(true)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum SetFlowTriggerResult {
    Success(SetFlowTriggerSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    PreconditionsNotMet(FlowPreconditionsNotMet),
    TypeIsNotSupported(FlowTypeIsNotSupported),
    InvalidTriggerInput(FlowInvalidTriggerInputError),
    InvalidTriggerStopPolicyInput(FlowInvalidTriggerStopPolicyInputError),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct SetFlowTriggerSuccess {
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

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct FlowInvalidTriggerStopPolicyInputError {
    pub reason: String,
}

#[ComplexObject]
impl FlowInvalidTriggerStopPolicyInputError {
    pub async fn message(&self) -> String {
        self.reason.clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
