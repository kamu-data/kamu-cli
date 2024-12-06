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
    CompactionRuleFull,
    CompactionRuleMetadataOnly,
    FlowConfigurationRule,
    FlowConfigurationService,
    FlowKeyDataset,
    IngestRule,
    ScheduleCronError,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    async fn set_config(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        config_input: FlowConfigurationInput,
    ) -> Result<SetFlowConfigResult> {
        let flow_run_config: FlowRunConfiguration = config_input.clone().into();
        if let Err(err) = flow_run_config.check_type_compatible(dataset_flow_type) {
            return Ok(SetFlowConfigResult::TypeIsNotSupported(err));
        };

        if let Some(e) = ensure_expected_dataset_kind(
            ctx,
            &self.dataset_handle,
            dataset_flow_type,
            Some(&flow_run_config),
        )
        .await?
        {
            return Ok(SetFlowConfigResult::IncompatibleDatasetKind(e));
        }

        let configuration_rule: FlowConfigurationRule = match config_input.try_into() {
            Ok(rule) => rule,
            Err(e) => return Ok(SetFlowConfigResult::FlowInvalidConfigInput(e)),
        };

        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        let res = flow_config_service
            .set_configuration(
                Utc::now(),
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
                configuration_rule,
            )
            .await
            .map_err(|e| match e {
                SetFlowConfigurationError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(SetFlowConfigResult::Success(SetFlowConfigSuccess {
            config: res.into(),
        }))
    }

    // #[graphql(guard = "LoggedInGuard::new()")]
    // async fn pause_flows(
    //     &self,
    //     ctx: &Context<'_>,
    //     dataset_flow_type: Option<DatasetFlowType>,
    // ) -> Result<bool> {
    //     ensure_scheduling_permission(ctx, &self.dataset_handle).await?;

    //     let flow_config_service = from_catalog::<dyn
    // FlowConfigurationService>(ctx).unwrap();

    //     flow_config_service
    //         .pause_dataset_flows(
    //             Utc::now(),
    //             &self.dataset_handle.id,
    //             dataset_flow_type.map(Into::into),
    //         )
    //         .await?;

    //     Ok(true)
    // }

    // #[graphql(guard = "LoggedInGuard::new()")]
    // async fn resume_flows(
    //     &self,
    //     ctx: &Context<'_>,
    //     dataset_flow_type: Option<DatasetFlowType>,
    // ) -> Result<bool> {
    //     ensure_scheduling_permission(ctx, &self.dataset_handle).await?;

    //     let flow_config_service = from_catalog::<dyn
    // FlowConfigurationService>(ctx).unwrap();

    //     flow_config_service
    //         .resume_dataset_flows(
    //             Utc::now(),
    //             &self.dataset_handle.id,
    //             dataset_flow_type.map(Into::into),
    //         )
    //         .await?;

    //     Ok(true)
    // }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum SetFlowConfigResult {
    Success(SetFlowConfigSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    PreconditionsNotMet(FlowPreconditionsNotMet),
    TypeIsNotSupported(FlowTypeIsNotSupported),
    FlowInvalidConfigInput(FlowInvalidConfigInputError),
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
pub(crate) struct FlowInvalidTransformConfig {
    reason: String,
}

#[ComplexObject]
impl FlowInvalidTransformConfig {
    pub async fn message(&self) -> String {
        self.reason.clone()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub(crate) struct FlowInvalidCompactionConfig {
    reason: String,
}

#[ComplexObject]
impl FlowInvalidCompactionConfig {
    pub async fn message(&self) -> String {
        self.reason.clone()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct FlowInvalidConfigInputError {
    reason: String,
}

#[ComplexObject]
impl FlowInvalidConfigInputError {
    pub async fn message(&self) -> String {
        self.reason.clone()
    }
}

impl From<ScheduleCronError> for FlowInvalidConfigInputError {
    fn from(value: ScheduleCronError) -> Self {
        Self {
            reason: value.to_string(),
        }
    }
}

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum SetFlowCompactionConfigResult {
    Success(SetFlowConfigSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    InvalidCompactionConfig(FlowInvalidCompactionConfig),
    TypeIsNotSupported(FlowTypeIsNotSupported),
}

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum SetFlowTransformConfigResult {
    Success(SetFlowConfigSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    InvalidTransformConfig(FlowInvalidTransformConfig),
    PreconditionsNotMet(FlowPreconditionsNotMet),
    TypeIsNotSupported(FlowTypeIsNotSupported),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<FlowConfigurationInput> for FlowConfigurationRule {
    type Error = FlowInvalidConfigInputError;

    fn try_from(value: FlowConfigurationInput) -> std::result::Result<Self, Self::Error> {
        match value {
            FlowConfigurationInput::Ingest(ingest_input) => {
                let ingest_rule: IngestRule = ingest_input.try_into()?;
                Ok(Self::IngestRule(ingest_rule))
            }
            FlowConfigurationInput::Compaction(compaction_input) => match compaction_input {
                CompactionConditionInput::Full(compaction_args) => CompactionRuleFull::new_checked(
                    compaction_args.max_slice_size,
                    compaction_args.max_slice_records,
                    compaction_args.recursive,
                )
                .map_err(|err| Self::Error {
                    reason: err.to_string(),
                })
                .map(|rule| Self::CompactionRule(rule.into())),
                CompactionConditionInput::MetadataOnly(compaction_args) => {
                    Ok(Self::CompactionRule(
                        CompactionRuleMetadataOnly {
                            recursive: compaction_args.recursive,
                        }
                        .into(),
                    ))
                }
            },
        }
    }
}
