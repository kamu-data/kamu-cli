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
    FlowConfigurationRule,
    FlowConfigurationService,
    FlowKeyDataset,
    Schedule,
    ScheduleTimeDelta,
    SetFlowConfigurationError,
    StartConditionConfiguration,
};
use opendatafabric as odf;

use super::{
    ensure_expected_dataset_kind,
    ensure_scheduling_permission,
    FlowIncompatibleDatasetKind,
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

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();
        let configuration_rule = match schedule {
            ScheduleInput::TimeDelta(td) => {
                Schedule::TimeDelta(ScheduleTimeDelta { every: td.into() })
            }
            ScheduleInput::CronExpression(cron_input) => {
                let cron_source = cron_input.into();
                Schedule::new_cron_schedule(cron_source).map_err(|e| GqlError::Gql(e.into()))?
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
        throttling_period: Option<TimeDeltaInput>,
        minimal_data_batch: Option<i32>,
    ) -> Result<SetFlowConfigResult> {
        if let Some(e) =
            ensure_expected_dataset_kind(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(SetFlowConfigResult::IncompatibleDatasetKind(e));
        }

        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        let res = flow_config_service
            .set_configuration(
                Utc::now(),
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                paused,
                FlowConfigurationRule::StartCondition(StartConditionConfiguration {
                    throttling_period: throttling_period.map(|tp| tp.into()),
                    minimal_data_batch,
                }),
            )
            .await
            .map_err(|e| match e {
                SetFlowConfigurationError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(SetFlowConfigResult::Success(SetFlowConfigSuccess {
            config: res.into(),
        }))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject)]
enum ScheduleInput {
    TimeDelta(TimeDeltaInput),
    CronExpression(CronExpressionInput),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
struct TimeDeltaInput {
    pub every: u32,
    pub unit: TimeUnit,
}

impl From<TimeDeltaInput> for chrono::Duration {
    fn from(value: TimeDeltaInput) -> Self {
        let every = value.every as i64;
        match value.unit {
            TimeUnit::Weeks => chrono::Duration::weeks(every),
            TimeUnit::Days => chrono::Duration::days(every),
            TimeUnit::Hours => chrono::Duration::hours(every),
            TimeUnit::Minutes => chrono::Duration::minutes(every),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
struct CronExpressionInput {
    pub min: String,
    pub hour: String,
    pub day_of_month: String,
    pub month: String,
    pub day_of_week: String,
}

impl From<CronExpressionInput> for kamu_flow_system::CronExpressionSource {
    fn from(value: CronExpressionInput) -> Self {
        Self {
            min: value.min,
            hour: value.hour,
            day_of_month: value.day_of_month,
            month: value.month,
            day_of_week: value.day_of_week,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum SetFlowConfigResult {
    Success(SetFlowConfigSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
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

///////////////////////////////////////////////////////////////////////////////
