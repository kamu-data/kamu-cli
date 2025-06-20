// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset::{
    FlowConfigRuleCompact,
    FlowConfigRuleCompactFull,
    FlowConfigRuleIngest,
};
use kamu_flow_system::{FlowBinding, FlowConfigurationRule, FlowConfigurationService};

use super::{
    FlowIncompatibleDatasetKind,
    FlowPreconditionsNotMet,
    ensure_expected_dataset_kind,
    ensure_flow_preconditions,
};
use crate::LoggedInGuard;
use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowConfigsMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetFlowConfigsMut<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    #[tracing::instrument(level = "info", name = DatasetFlowConfigsMut_set_config, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_config(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
        config_input: FlowConfigurationInput,
    ) -> Result<SetFlowConfigResult> {
        let flow_run_config: FlowRunConfiguration = config_input.into();
        if let Err(err) = flow_run_config.check_type_compatible(dataset_flow_type) {
            return Ok(SetFlowConfigResult::TypeIsNotSupported(err));
        }

        if let Some(e) = ensure_expected_dataset_kind(
            ctx,
            self.dataset_request_state,
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

        if let Some(e) =
            ensure_flow_preconditions(ctx, self.dataset_request_state, dataset_flow_type, None)
                .await?
        {
            return Ok(SetFlowConfigResult::PreconditionsNotMet(e));
        }

        let flow_config_service = from_catalog_n!(ctx, dyn FlowConfigurationService);

        let flow_binding = FlowBinding::new_dataset(
            self.dataset_request_state.dataset_id().clone(),
            map_dataset_flow_type(dataset_flow_type),
        );

        let res = flow_config_service
            .set_configuration(flow_binding, configuration_rule)
            .await
            .int_err()?;

        Ok(SetFlowConfigResult::Success(SetFlowConfigSuccess {
            config: res.into(),
        }))
    }
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

#[derive(Debug)]
pub struct FlowTypeIsNotSupported;

#[Object]
impl FlowTypeIsNotSupported {
    pub async fn message(&self) -> String {
        "Flow type is not supported".to_string()
    }
}

#[derive(SimpleObject, Debug)]
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<FlowConfigurationInput> for FlowConfigurationRule {
    type Error = FlowInvalidConfigInputError;

    fn try_from(value: FlowConfigurationInput) -> std::result::Result<Self, Self::Error> {
        match value {
            FlowConfigurationInput::Ingest(ingest_input) => {
                let ingest_rule: FlowConfigRuleIngest = ingest_input.into();
                Ok(ingest_rule.into_flow_config())
            }
            FlowConfigurationInput::Compaction(compaction_input) => match compaction_input {
                CompactionConditionInput::Full(compaction_args) => {
                    FlowConfigRuleCompactFull::new_checked(
                        compaction_args.max_slice_size,
                        compaction_args.max_slice_records,
                        compaction_args.recursive,
                    )
                    .map_err(|err| Self::Error {
                        reason: err.to_string(),
                    })
                    .map(|rule| FlowConfigRuleCompact::Full(rule).into_flow_config())
                }
                CompactionConditionInput::MetadataOnly(compaction_args) => {
                    let compaction_rule = FlowConfigRuleCompact::MetadataOnly {
                        recursive: compaction_args.recursive,
                    };
                    Ok(compaction_rule.into_flow_config())
                }
            },
        }
    }
}
