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
    CompactingRule,
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
    ensure_set_config_flow_supported,
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
        if !ensure_set_config_flow_supported(dataset_flow_type, std::any::type_name::<Schedule>()) {
            return Ok(SetFlowConfigResult::TypeIsNotSupported(
                FlowTypeIsNotSupported,
            ));
        }
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
    ) -> Result<SetFlowBatchingConfigResult> {
        if !ensure_set_config_flow_supported(
            dataset_flow_type,
            std::any::type_name::<BatchingRule>(),
        ) {
            return Ok(SetFlowBatchingConfigResult::TypeIsNotSupported(
                FlowTypeIsNotSupported,
            ));
        }
        let batching_rule = match BatchingRule::new_checked(
            batching.min_records_to_await,
            batching.max_batching_interval.into(),
        ) {
            Ok(rule) => rule,
            Err(e) => {
                return Ok(SetFlowBatchingConfigResult::InvalidBatchingConfig(
                    FlowInvalidBatchingConfig {
                        reason: e.to_string(),
                    },
                ))
            }
        };

        if let Some(e) =
            ensure_expected_dataset_kind(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(SetFlowBatchingConfigResult::IncompatibleDatasetKind(e));
        }

        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;
        if let Some(e) =
            ensure_flow_preconditions(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(SetFlowBatchingConfigResult::PreconditionsNotMet(e));
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

        Ok(SetFlowBatchingConfigResult::Success(SetFlowConfigSuccess {
            config: res.into(),
        }))
    }

    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_config_compacting(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        compacting_args: CompactingConditionInput,
    ) -> Result<SetFlowCompactingConfigResult> {
        if !ensure_set_config_flow_supported(
            dataset_flow_type,
            std::any::type_name::<CompactingRule>(),
        ) {
            return Ok(SetFlowCompactingConfigResult::TypeIsNotSupported(
                FlowTypeIsNotSupported,
            ));
        }
        let compacting_rule = match CompactingRule::new_checked(
            compacting_args.max_slice_size,
            compacting_args.max_slice_records,
            compacting_args.keep_metadata_only,
        ) {
            Ok(rule) => rule,
            Err(e) => {
                return Ok(SetFlowCompactingConfigResult::InvalidCompactingConfig(
                    FlowInvalidCompactingConfig {
                        reason: e.to_string(),
                    },
                ))
            }
        };

        if let Some(e) =
            ensure_expected_dataset_kind(ctx, &self.dataset_handle, dataset_flow_type).await?
        {
            return Ok(SetFlowCompactingConfigResult::IncompatibleDatasetKind(e));
        }
        ensure_scheduling_permission(ctx, &self.dataset_handle).await?;

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        let res = flow_config_service
            .set_configuration(
                Utc::now(),
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                false,
                FlowConfigurationRule::CompactingRule(compacting_rule),
            )
            .await
            .map_err(|e| match e {
                SetFlowConfigurationError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(SetFlowCompactingConfigResult::Success(
            SetFlowConfigSuccess { config: res.into() },
        ))
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

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum SetFlowConfigResult {
    Success(SetFlowConfigSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    PreconditionsNotMet(FlowPreconditionsNotMet),
    TypeIsNotSupported(FlowTypeIsNotSupported),
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

#[derive(Debug, Clone)]
pub struct FlowTypeIsNotSupported;

#[Object]
impl FlowTypeIsNotSupported {
    pub async fn message(&self) -> String {
        "Flow type is not supported".to_string()
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

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub(crate) struct FlowInvalidCompactingConfig {
    reason: String,
}

#[ComplexObject]
impl FlowInvalidCompactingConfig {
    pub async fn message(&self) -> String {
        self.reason.clone()
    }
}

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum SetFlowCompactingConfigResult {
    Success(SetFlowConfigSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    InvalidCompactingConfig(FlowInvalidCompactingConfig),
    TypeIsNotSupported(FlowTypeIsNotSupported),
}

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum SetFlowBatchingConfigResult {
    Success(SetFlowConfigSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    InvalidBatchingConfig(FlowInvalidBatchingConfig),
    PreconditionsNotMet(FlowPreconditionsNotMet),
    TypeIsNotSupported(FlowTypeIsNotSupported),
}

///////////////////////////////////////////////////////////////////////////////
