// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use {kamu_adapter_flow_dataset as afs, kamu_flow_system as fs};

use super::{FlowIncompatibleDatasetKind, FlowPreconditionsNotMet, ensure_flow_preconditions};
use crate::LoggedInGuard;
use crate::mutations::ensure_flow_applied_to_expected_dataset_kind;
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

    #[tracing::instrument(level = "info", name = DatasetFlowConfigsMut_set_ingest_config, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_ingest_config(
        &self,
        ctx: &Context<'_>,
        ingest_config_input: FlowConfigIngestInput,
        retry_policy_input: Option<FlowRetryPolicyInput>,
    ) -> Result<SetFlowConfigResult> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        if let Err(e) =
            ensure_flow_applied_to_expected_dataset_kind(resolved_dataset, odf::DatasetKind::Root)
        {
            return Ok(SetFlowConfigResult::IncompatibleDatasetKind(e));
        }

        if let Some(e) = ensure_flow_preconditions(
            ctx,
            self.dataset_request_state,
            DatasetFlowType::Ingest,
            None,
        )
        .await?
        {
            return Ok(SetFlowConfigResult::PreconditionsNotMet(e));
        }

        let ingest_config_rule: afs::FlowConfigRuleIngest = ingest_config_input.into();
        let configuration_rule = ingest_config_rule.into_flow_config();

        let retry_policy: Option<fs::RetryPolicy> = retry_policy_input.map(Into::into);

        let flow_config_service = from_catalog_n!(ctx, dyn fs::FlowConfigurationService);

        let flow_binding = fs::FlowBinding::for_dataset(
            self.dataset_request_state.dataset_id().clone(),
            afs::FLOW_TYPE_DATASET_INGEST,
        );

        let res = flow_config_service
            .set_configuration(flow_binding, configuration_rule, retry_policy)
            .await
            .int_err()?;

        Ok(SetFlowConfigResult::Success(SetFlowConfigSuccess {
            config: res.into(),
        }))
    }

    #[tracing::instrument(level = "info", name = DatasetFlowConfigsMut_set_ingest_config, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_compaction_config(
        &self,
        ctx: &Context<'_>,
        compaction_config_input: FlowConfigCompactionInput,
    ) -> Result<SetFlowConfigResult> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        if matches!(compaction_config_input, FlowConfigCompactionInput::Full(_))
            && let Err(e) = ensure_flow_applied_to_expected_dataset_kind(
                resolved_dataset,
                odf::DatasetKind::Root,
            )
        {
            return Ok(SetFlowConfigResult::IncompatibleDatasetKind(e));
        }

        if let Some(e) = ensure_flow_preconditions(
            ctx,
            self.dataset_request_state,
            DatasetFlowType::HardCompaction,
            None,
        )
        .await?
        {
            return Ok(SetFlowConfigResult::PreconditionsNotMet(e));
        }

        let compact_config_rule =
            match afs::FlowConfigRuleCompact::try_from(compaction_config_input) {
                Ok(compact_config_rule) => compact_config_rule,
                Err(e) => {
                    return Ok(SetFlowConfigResult::FlowInvalidConfigInput(
                        FlowInvalidConfigInputError { reason: e },
                    ));
                }
            };
        let configuration_rule = compact_config_rule.into_flow_config();

        let flow_config_service = from_catalog_n!(ctx, dyn fs::FlowConfigurationService);

        let flow_binding = fs::FlowBinding::for_dataset(
            self.dataset_request_state.dataset_id().clone(),
            afs::FLOW_TYPE_DATASET_COMPACT,
        );

        let res = flow_config_service
            .set_configuration(
                flow_binding,
                configuration_rule,
                None, /* no retries for compacting */
            )
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
