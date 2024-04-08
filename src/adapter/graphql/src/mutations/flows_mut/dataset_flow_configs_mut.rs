// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_flow_system::{
    BatchingRule,
    FlowConfigurationRule,
    FlowConfigurationService,
    FlowKeyDataset,
    Schedule,
    ScheduleTimeDelta,
    SetFlowConfigurationError,
};
use opendatafabric as odf;

use super::{
    ensure_expected_dataset_kind,
    ensure_flow_preconditions,
    ensure_scheduling_permission,
    FlowIncompatibleDatasetKind,
    FlowPreconditionsNotMet,
};
use crate::prelude::*;
use crate::LoggedInGuard;

///////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowConfigsMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetFlowConfigsMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_config_schedule(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        paused: bool,
        schedule: ScheduleInput,
    ) -> Result<SetFlowConfigResult> {
        if let Some(e) =
            ensure_expected_dataset_kind(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(SetFlowConfigResult::IncompatibleDatasetKind(e));
        }

        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;
        if let Some(e) =
            ensure_flow_preconditions(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(SetFlowConfigResult::PreconditionsNotMet(e));
        }

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();
        let configuration_rule = match schedule {
            ScheduleInput::TimeDelta(td) => {
                Schedule::TimeDelta(ScheduleTimeDelta { every: td.into() })
            }
            ScheduleInput::Cron5ComponentExpression(cron_5component_expression) => {
                Schedule::try_from_5component_cron_expression(&cron_5component_expression)
                    .map_err(|e| GqlError::Gql(e.into()))?
            }
        };

        let res = flow_config_service
            .set_configuration(
                Utc::now(),
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                paused,
                FlowConfigurationRule::Schedule(configuration_rule),
            )
            .await
            .map_err(|e| match e {
                SetFlowConfigurationError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(SetFlowConfigResult::Success(SetFlowConfigSuccess {
            config: res.into(),
        }))
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_config_batching(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        paused: bool,
        batching: BatchingConditionInput,
    ) -> Result<SetFlowConfigResult> {
        let batching_rule = match BatchingRule::new_checked(
            batching.min_records_to_await,
            batching.max_batching_interval.into(),
        ) {
            Ok(rule) => rule,
            Err(e) => {
                return Ok(SetFlowConfigResult::InvalidBatchingConfig(
                    FlowInvalidBatchingConfig {
                        reason: e.to_string(),
                    },
                ))
            }
        };

        if let Some(e) =
            ensure_expected_dataset_kind(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(SetFlowConfigResult::IncompatibleDatasetKind(e));
        }

        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;
        if let Some(e) =
            ensure_flow_preconditions(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(SetFlowConfigResult::PreconditionsNotMet(e));
        }

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        let res = flow_config_service
            .set_configuration(
                Utc::now(),
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                paused,
                FlowConfigurationRule::BatchingRule(batching_rule),
            )
            .await
            .map_err(|e| match e {
                SetFlowConfigurationError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(SetFlowConfigResult::Success(SetFlowConfigSuccess {
            config: res.into(),
        }))
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn pause_flows(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<bool> {
        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        flow_config_service
            .pause_dataset_flows(
                Utc::now(),
                &self.dataset_handle.id,
                dataset_flow_type.map(Into::into),
            )
            .await?;

        Ok(true)
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resume_flows(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<bool> {
        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        flow_config_service
            .resume_dataset_flows(
                Utc::now(),
                &self.dataset_handle.id,
                dataset_flow_type.map(Into::into),
            )
            .await?;

        Ok(true)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject)]
enum ScheduleInput {
    TimeDelta(TimeDeltaInput),
    /// Supported CRON syntax: min hour dayOfMonth month dayOfWeek
    Cron5ComponentExpression(String),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
struct TimeDeltaInput {
    pub every: u32,
    pub unit: TimeUnit,
}

impl From<TimeDeltaInput> for chrono::Duration {
    fn from(value: TimeDeltaInput) -> Self {
        let every = i64::from(value.every);
        match value.unit {
            TimeUnit::Weeks => chrono::Duration::try_weeks(every).unwrap(),
            TimeUnit::Days => chrono::Duration::try_days(every).unwrap(),
            TimeUnit::Hours => chrono::Duration::try_hours(every).unwrap(),
            TimeUnit::Minutes => chrono::Duration::try_minutes(every).unwrap(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
struct BatchingConditionInput {
    pub min_records_to_await: u64,
    pub max_batching_interval: TimeDeltaInput,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum SetFlowConfigResult {
    Success(SetFlowConfigSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    InvalidBatchingConfig(FlowInvalidBatchingConfig),
    PreconditionsNotMet(FlowPreconditionsNotMet),
}

#[derive(SimpleObject)]
#[graphql(complex)]
struct SetFlowConfigSuccess {
    pub config: FlowConfiguration,
}

#[ComplexObject]
impl SetFlowConfigSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub(crate) struct FlowInvalidBatchingConfig {
    reason: String,
}

#[ComplexObject]
impl FlowInvalidBatchingConfig {
    pub async fn message(&self) -> String {
        self.reason.clone()
    }
}

///////////////////////////////////////////////////////////////////////////////
