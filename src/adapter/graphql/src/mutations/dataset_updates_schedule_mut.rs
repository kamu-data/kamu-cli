// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::GetSummaryOpts;
use kamu_dataset_update_flow::*;
use opendatafabric as odf;

use crate::prelude::*;
use crate::queries::TimeUnit;
use crate::{utils, LoggedInGuard};

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetUpdatesScheduleMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetUpdatesScheduleMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_polling_time_delta(
        &self,
        ctx: &Context<'_>,
        paused: bool,
        every: TimeDeltaInput,
    ) -> Result<SetUpdateScheduleResult> {
        self.ensure_expected_dataset_kind(ctx, opendatafabric::DatasetKind::Root)
            .await?;
        self.ensure_scheduling_permission(ctx).await?;

        let update_schedule_service = from_catalog::<dyn UpdateScheduleService>(ctx).unwrap();

        let update_state = update_schedule_service
            .set_schedule(
                self.dataset_handle.id.clone(),
                Schedule::TimeDelta(ScheduleTimeDelta {
                    every: every.into(),
                }),
            )
            .await
            .map_err(|e| match e {
                SetScheduleError::Internal(e) => GqlError::Internal(e),
            })?;

        self.apply_pause_resume_schedule(update_schedule_service.as_ref(), paused, &update_state)
            .await?;

        Ok(SetUpdateScheduleResult::Success(
            SetUpdateScheduleResultSuccess { success: true },
        ))
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_polling_cron_expression(
        &self,
        ctx: &Context<'_>,
        paused: bool,
        cron_expression: String,
    ) -> Result<SetUpdateScheduleResult> {
        self.ensure_expected_dataset_kind(ctx, opendatafabric::DatasetKind::Root)
            .await?;
        self.ensure_scheduling_permission(ctx).await?;

        let update_schedule_service = from_catalog::<dyn UpdateScheduleService>(ctx).unwrap();

        let update_state = update_schedule_service
            .set_schedule(
                self.dataset_handle.id.clone(),
                Schedule::CronExpression(ScheduleCronExpression {
                    expression: cron_expression,
                }),
            )
            .await
            .map_err(|e| match e {
                SetScheduleError::Internal(e) => GqlError::Internal(e),
            })?;

        self.apply_pause_resume_schedule(update_schedule_service.as_ref(), paused, &update_state)
            .await?;

        Ok(SetUpdateScheduleResult::Success(
            SetUpdateScheduleResultSuccess { success: true },
        ))
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_throttling(
        &self,
        ctx: &Context<'_>,
        paused: bool,
        throttling_period: Option<TimeDeltaInput>,
        minimal_data_batch: Option<i32>,
    ) -> Result<SetUpdateScheduleResult> {
        self.ensure_expected_dataset_kind(ctx, opendatafabric::DatasetKind::Derivative)
            .await?;
        self.ensure_scheduling_permission(ctx).await?;

        let update_schedule_service = from_catalog::<dyn UpdateScheduleService>(ctx).unwrap();

        let update_state = update_schedule_service
            .set_schedule(
                self.dataset_handle.id.clone(),
                Schedule::Reactive(ScheduleReactive {
                    throttling_period: throttling_period.map(|tp| tp.into()),
                    minimal_data_batch,
                }),
            )
            .await
            .map_err(|e| match e {
                SetScheduleError::Internal(e) => GqlError::Internal(e),
            })?;

        self.apply_pause_resume_schedule(update_schedule_service.as_ref(), paused, &update_state)
            .await?;

        Ok(SetUpdateScheduleResult::Success(
            SetUpdateScheduleResultSuccess { success: true },
        ))
    }

    #[graphql(skip)]
    async fn ensure_expected_dataset_kind(
        &self,
        ctx: &Context<'_>,
        expected_kind: opendatafabric::DatasetKind,
    ) -> Result<()> {
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
        }

        Ok(())
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

    #[graphql(skip)]
    async fn apply_pause_resume_schedule(
        &self,
        update_schedule_service: &dyn UpdateScheduleService,
        paused: bool,
        update_state: &UpdateScheduleState,
    ) -> Result<()> {
        if paused && !update_state.paused() {
            update_schedule_service
                .pause_schedule(&self.dataset_handle.id)
                .await
                .map_err(|e| {
                    GqlError::Internal(match e {
                        PauseScheduleError::Internal(e) => e,
                        PauseScheduleError::NotFound(e) => e.int_err(),
                    })
                })?;
        } else if !paused && update_state.paused() {
            update_schedule_service
                .resume_schedule(&self.dataset_handle.id)
                .await
                .map_err(|e| {
                    GqlError::Internal(match e {
                        ResumeScheduleError::Internal(e) => e,
                        ResumeScheduleError::NotFound(e) => e.int_err(),
                    })
                })?;
        }

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

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum SetUpdateScheduleResult {
    Success(SetUpdateScheduleResultSuccess),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct SetUpdateScheduleResultSuccess {
    success: bool,
}

#[ComplexObject]
impl SetUpdateScheduleResultSuccess {
    async fn message(&self) -> String {
        format!("Success")
    }
}

////////////////////////////////////////////////////////////////////////////////////////
