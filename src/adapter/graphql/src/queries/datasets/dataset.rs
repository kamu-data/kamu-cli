// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use futures::TryStreamExt;
use kamu_core::{self as domain, MetadataChainExt, TryStreamExtExt};
use kamu_dataset_update_flow::{
    Schedule,
    ScheduleCronExpression,
    ScheduleReactive,
    UpdateScheduleService,
};
use opendatafabric as odf;

use crate::prelude::*;
use crate::queries::*;
use crate::utils;

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Dataset {
    owner: Account,
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl Dataset {
    #[graphql(skip)]
    pub fn new(owner: Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            owner,
            dataset_handle,
        }
    }

    #[graphql(skip)]
    pub async fn from_ref(ctx: &Context<'_>, dataset_ref: &odf::DatasetRef) -> Result<Dataset> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();

        // TODO: Should we resolve reference at this point or allow unresolved and fail
        // later?
        let hdl = dataset_repo
            .resolve_dataset_ref(dataset_ref)
            .await
            .int_err()?;
        Ok(Dataset::new(
            Account::from_dataset_alias(ctx, &hdl.alias),
            hdl,
        ))
    }

    #[graphql(skip)]
    async fn get_dataset(&self, ctx: &Context<'_>) -> Result<std::sync::Arc<dyn domain::Dataset>> {
        utils::get_dataset(ctx, &self.dataset_handle)
            .await
            .map_err(|e| e.into())
    }

    /// Unique identifier of the dataset
    async fn id(&self) -> DatasetID {
        self.dataset_handle.id.clone().into()
    }

    /// Symbolic name of the dataset.
    /// Name can change over the dataset's lifetime. For unique identifier use
    /// `id()`.
    async fn name(&self) -> DatasetName {
        self.dataset_handle.alias.dataset_name.clone().into()
    }

    /// Returns the user or organization that owns this dataset
    async fn owner(&self) -> &Account {
        &self.owner
    }

    /// Returns dataset alias (user + name)
    async fn alias(&self) -> DatasetAlias {
        self.dataset_handle.alias.clone().into()
    }

    /// Returns the kind of a dataset (Root or Derivative)
    async fn kind(&self, ctx: &Context<'_>) -> Result<DatasetKind> {
        let dataset = self.get_dataset(ctx).await?;
        let summary = dataset
            .get_summary(domain::GetSummaryOpts::default())
            .await
            .int_err()?;
        Ok(summary.kind.into())
    }

    /// Access to the data of the dataset
    async fn data(&self) -> DatasetData {
        DatasetData::new(self.dataset_handle.clone())
    }

    /// Access to the metadata of the dataset
    async fn metadata(&self) -> DatasetMetadata {
        DatasetMetadata::new(self.dataset_handle.clone())
    }

    // TODO: PERF: Avoid traversing the entire chain
    /// Creation time of the first metadata block in the chain
    async fn created_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let dataset = self.get_dataset(ctx).await?;
        let seed = dataset
            .as_metadata_chain()
            .iter_blocks_ref(&domain::BlockRef::Head)
            .map_ok(|(_, b)| b)
            .try_last()
            .await
            .int_err()?
            .expect("Dataset without blocks");
        Ok(seed.system_time)
    }

    /// Creation time of the most recent metadata block in the chain
    async fn last_updated_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let dataset = self.get_dataset(ctx).await?;
        let head = dataset
            .as_metadata_chain()
            .iter_blocks_ref(&domain::BlockRef::Head)
            .map_ok(|(_, b)| b)
            .try_first()
            .await
            .int_err()?
            .expect("Dataset without blocks");
        Ok(head.system_time)
    }

    /// Permissions of the current user
    async fn permissions(&self, ctx: &Context<'_>) -> Result<DatasetPermissions> {
        use kamu_core::auth;
        let dataset_action_authorizer =
            from_catalog::<dyn auth::DatasetActionAuthorizer>(ctx).unwrap();

        let allowed_actions = dataset_action_authorizer
            .get_allowed_actions(&self.dataset_handle)
            .await;
        let can_read = allowed_actions.contains(&auth::DatasetAction::Read);
        let can_write = allowed_actions.contains(&auth::DatasetAction::Write);

        Ok(DatasetPermissions {
            can_view: can_read,
            can_delete: can_write,
            can_rename: can_write,
            can_commit: can_write,
            can_schedule: can_write,
        })
    }

    /// Configured update schedule
    async fn updates_schedule(&self, ctx: &Context<'_>) -> Result<Option<DatasetUpdatesSchedule>> {
        use kamu_core::auth;
        let dataset_action_authorizer =
            from_catalog::<dyn auth::DatasetActionAuthorizer>(ctx).unwrap();

        dataset_action_authorizer
            .check_action_allowed(&self.dataset_handle, auth::DatasetAction::Read)
            .await
            .map_err(|_| {
                GqlError::Gql(
                    Error::new("Dataset access error").extend_with(|_, eev| {
                        eev.set("alias", self.dataset_handle.alias.to_string())
                    }),
                )
            })?;

        let update_schedule_service = from_catalog::<dyn UpdateScheduleService>(ctx).unwrap();
        let maybe_update_schedule = update_schedule_service
            .find_schedule(&self.dataset_handle.id)
            .await
            .int_err()?;

        Ok(
            maybe_update_schedule.map(|update_schedule| DatasetUpdatesSchedule {
                paused: update_schedule.paused(),
                update_settings: match update_schedule.schedule() {
                    Schedule::None => unreachable!("May only be applied to removed dataset"),
                    Schedule::TimeDelta(time_delta) => {
                        DatasetUpdatesSettings::Polling(DatasetUpdatesSettingsPolling {
                            schedule: DatasetUpdatesPollingSchedule::TimeDelta(
                                time_delta.every.into(),
                            ),
                        })
                    }
                    Schedule::CronExpression(cron) => {
                        DatasetUpdatesSettings::Polling(DatasetUpdatesSettingsPolling {
                            schedule: DatasetUpdatesPollingSchedule::Cron(cron.into()),
                        })
                    }
                    Schedule::Reactive(reactive) => {
                        DatasetUpdatesSettings::Throttling(reactive.into())
                    }
                },
            }),
        )
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetPermissions {
    can_view: bool,
    can_delete: bool,
    can_rename: bool,
    can_commit: bool,
    can_schedule: bool,
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetUpdatesSchedule {
    pub paused: bool,
    pub update_settings: DatasetUpdatesSettings,
}

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum DatasetUpdatesSettings {
    Polling(DatasetUpdatesSettingsPolling),
    Throttling(DatasetUpdatesSettingsThrottling),
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetUpdatesSettingsPolling {
    pub schedule: DatasetUpdatesPollingSchedule,
}

#[derive(Union, Debug, Clone, PartialEq, Eq)]
pub enum DatasetUpdatesPollingSchedule {
    TimeDelta(TimeDelta),
    Cron(CronExpression),
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct CronExpression {
    pub expression: String,
}

impl From<ScheduleCronExpression> for CronExpression {
    fn from(value: ScheduleCronExpression) -> Self {
        Self {
            expression: value.expression,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetUpdatesSettingsThrottling {
    pub throttling_period: Option<TimeDelta>,
    pub minimal_data_batch: Option<i32>,
}

impl From<ScheduleReactive> for DatasetUpdatesSettingsThrottling {
    fn from(value: ScheduleReactive) -> Self {
        Self {
            throttling_period: value.throttling_period.map(|tp| tp.into()),
            minimal_data_batch: value.minimal_data_batch,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct TimeDelta {
    pub every: u32,
    pub unit: TimeUnit,
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Minutes,
    Hours,
    Days,
    Weeks,
}

impl From<chrono::Duration> for TimeDelta {
    fn from(value: chrono::Duration) -> Self {
        let num_weeks = value.num_weeks();
        if num_weeks > 0 {
            assert!((value - chrono::Duration::weeks(num_weeks)).is_zero());
            return Self {
                every: num_weeks as u32,
                unit: TimeUnit::Weeks,
            };
        }

        let num_days = value.num_days();
        if num_days > 0 {
            assert!((value - chrono::Duration::days(num_days)).is_zero());
            return Self {
                every: num_days as u32,
                unit: TimeUnit::Days,
            };
        }

        let num_hours = value.num_hours();
        if num_hours > 0 {
            assert!((value - chrono::Duration::hours(num_hours)).is_zero());
            return Self {
                every: num_hours as u32,
                unit: TimeUnit::Hours,
            };
        }

        let num_minutes = value.num_minutes();
        if num_minutes > 0 {
            assert!((value - chrono::Duration::minutes(num_minutes)).is_zero());
            return Self {
                every: num_minutes as u32,
                unit: TimeUnit::Minutes,
            };
        }

        unreachable!("Expecting intervals not tinier than 1 minute");
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
