// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_core::GetSummaryOpts;
use kamu_flow_system::{
    FlowConfigurationRule,
    FlowConfigurationService,
    FlowKeyDataset,
    Schedule,
    ScheduleCronExpression,
    ScheduleTimeDelta,
    SetFlowConfigurationError,
    StartConditionConfiguration,
};
use opendatafabric as odf;

use crate::prelude::*;
use crate::{utils, LoggedInGuard};

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
        if let Some(e) = self
            .ensure_expected_dataset_kind(ctx, dataset_flow_type)
            .await?
        {
            return Ok(SetFlowConfigResult::IncompatibleDatasetKind(e));
        }

        self.ensure_scheduling_permission(ctx).await?;

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        let res = flow_config_service
            .set_configuration(
                Utc::now(),
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                paused,
                FlowConfigurationRule::Schedule(match schedule {
                    ScheduleInput::TimeDelta(td) => {
                        Schedule::TimeDelta(ScheduleTimeDelta { every: td.into() })
                    }
                    ScheduleInput::CronExpression(cron_expression) => {
                        Schedule::CronExpression(ScheduleCronExpression { cron_expression })
                    }
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

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_config_batching(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        paused: bool,
        throttling_period: Option<TimeDeltaInput>,
        minimal_data_batch: Option<i32>,
    ) -> Result<SetFlowConfigResult> {
        if let Some(e) = self
            .ensure_expected_dataset_kind(ctx, dataset_flow_type)
            .await?
        {
            return Ok(SetFlowConfigResult::IncompatibleDatasetKind(e));
        }

        self.ensure_scheduling_permission(ctx).await?;

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

    #[graphql(skip)]
    async fn ensure_expected_dataset_kind(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
    ) -> Result<Option<SetFlowConfigIncompatibleDatasetKind>> {
        let dataset_flow_type: kamu_flow_system::DatasetFlowType = dataset_flow_type.into();
        match dataset_flow_type.dataset_kind_restriction() {
            Some(expected_kind) => {
                let dataset = utils::get_dataset(ctx, &self.dataset_handle).await?;

                let dataset_kind = dataset
                    .get_summary(GetSummaryOpts::default())
                    .await
                    .int_err()?
                    .kind;

                if dataset_kind != expected_kind {
                    Ok(Some(SetFlowConfigIncompatibleDatasetKind {
                        expected_dataset_kind: expected_kind.into(),
                        actual_dataset_kind: dataset_kind.into(),
                    }))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    #[graphql(skip)]
    async fn ensure_scheduling_permission(&self, ctx: &Context<'_>) -> Result<()> {
        use kamu_core::auth;
        let dataset_action_authorizer =
            from_catalog::<dyn auth::DatasetActionAuthorizer>(ctx).unwrap();

        dataset_action_authorizer
            .check_action_allowed(&self.dataset_handle, auth::DatasetAction::Write)
            .await
            .map_err(|_| {
                GqlError::Gql(
                    Error::new("Dataset access error").extend_with(|_, eev| {
                        eev.set("alias", self.dataset_handle.alias.to_string())
                    }),
                )
            })?;

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject)]
enum ScheduleInput {
    TimeDelta(TimeDeltaInput),
    CronExpression(String),
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

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum SetFlowConfigResult {
    Success(SetFlowConfigSuccess),
    IncompatibleDatasetKind(SetFlowConfigIncompatibleDatasetKind),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct SetFlowConfigSuccess {
    pub config: FlowConfiguration,
}

#[ComplexObject]
impl SetFlowConfigSuccess {
    async fn message(&self) -> String {
        format!("Success")
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct SetFlowConfigIncompatibleDatasetKind {
    pub expected_dataset_kind: DatasetKind,
    pub actual_dataset_kind: DatasetKind,
}

#[ComplexObject]
impl SetFlowConfigIncompatibleDatasetKind {
    async fn message(&self) -> String {
        format!(
            "Expected a {} dataset, but a {} dataset was provided",
            self.expected_dataset_kind, self.actual_dataset_kind,
        )
    }
}

impl std::fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                DatasetKind::Root => "Root",
                DatasetKind::Derivative => "Derivative",
            }
        )
    }
}

///////////////////////////////////////////////////////////////////////////////
