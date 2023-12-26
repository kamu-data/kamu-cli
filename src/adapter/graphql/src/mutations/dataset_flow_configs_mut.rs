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
use crate::queries::{DatasetFlowType, TimeUnit};
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
    async fn set_config_time_delta(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        paused: bool,
        every: TimeDeltaInput,
    ) -> Result<bool> {
        self.ensure_expected_dataset_kind(ctx, dataset_flow_type)
            .await?;
        self.ensure_scheduling_permission(ctx).await?;

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        flow_config_service
            .set_configuration(
                Utc::now(),
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                paused,
                FlowConfigurationRule::Schedule(Schedule::TimeDelta(ScheduleTimeDelta {
                    every: every.into(),
                })),
            )
            .await
            .map_err(|e| match e {
                SetFlowConfigurationError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(true)
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_config_cron_expression(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        paused: bool,
        cron_expression: String,
    ) -> Result<bool> {
        self.ensure_expected_dataset_kind(ctx, dataset_flow_type)
            .await?;
        self.ensure_scheduling_permission(ctx).await?;

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        flow_config_service
            .set_configuration(
                Utc::now(),
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                paused,
                FlowConfigurationRule::Schedule(Schedule::CronExpression(ScheduleCronExpression {
                    expression: cron_expression,
                })),
            )
            .await
            .map_err(|e| match e {
                SetFlowConfigurationError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(true)
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_config_batching(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        paused: bool,
        throttling_period: Option<TimeDeltaInput>,
        minimal_data_batch: Option<i32>,
    ) -> Result<bool> {
        self.ensure_expected_dataset_kind(ctx, dataset_flow_type)
            .await?;
        self.ensure_scheduling_permission(ctx).await?;

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        flow_config_service
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

        Ok(true)
    }

    #[graphql(skip)]
    async fn ensure_expected_dataset_kind(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
    ) -> Result<()> {
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
                    return Err(GqlError::Gql(Error::new(format!(
                        "Expected {:?} dataset kind",
                        expected_kind
                    ))));
                } else {
                    Ok(())
                }
            }
            None => Ok(()),
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

////////////////////////////////////////////////////////////////////////////////////////

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
