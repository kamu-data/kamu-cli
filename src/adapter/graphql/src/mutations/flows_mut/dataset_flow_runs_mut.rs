// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset as afs;
use kamu_flow_system as fs;

use super::{
    FlowInDatasetError,
    FlowIncompatibleDatasetKind,
    FlowNotFound,
    FlowPreconditionsNotMet,
    check_if_flow_belongs_to_dataset,
    ensure_flow_preconditions,
};
use crate::mutations::ensure_flow_applied_to_expected_dataset_kind;
use crate::prelude::*;
use crate::queries::{DatasetRequestState, Flow};
use crate::{LoggedInGuard, utils};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowRunsMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

/// Mutations that trigger or cancel flow runs of a dataset
#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetFlowRunsMut<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Manually triggers an ingest flow of a root dataset
    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_ingest_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn trigger_ingest_flow(
        &self,
        ctx: &Context<'_>,
        ingest_config_input: Option<FlowConfigIngestInput>,
    ) -> Result<TriggerFlowResult> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        if let Err(e) =
            ensure_flow_applied_to_expected_dataset_kind(resolved_dataset, odf::DatasetKind::Root)
        {
            return Ok(TriggerFlowResult::IncompatibleDatasetKind(e));
        }

        if let Some(e) = ensure_flow_preconditions(
            ctx,
            self.dataset_request_state,
            DatasetFlowType::Ingest,
            None,
        )
        .await?
        {
            return Ok(TriggerFlowResult::PreconditionsNotMet(e));
        }

        // TODO: for some datasets launching manually might not be an option:
        //   i.e., root datasets with push sources require input data to arrive

        let flow_run_service = from_catalog_n!(ctx, dyn fs::FlowRunService);
        let logged_account = utils::get_logged_account(ctx);
        let dataset_handle = self.dataset_request_state.dataset_handle();

        let maybe_ingest_config_rule: Option<afs::FlowConfigRuleIngest> =
            ingest_config_input.map(Into::into);
        let maybe_forced_configuration_rule =
            maybe_ingest_config_rule.map(afs::FlowConfigRuleIngest::into_flow_config);

        let flow_binding = afs::ingest_dataset_binding(&dataset_handle.id);

        let flow_state = flow_run_service
            .run_flow_manually(
                utils::system_time_now(ctx),
                &flow_binding,
                logged_account.account_handle.did,
                maybe_forced_configuration_rule,
            )
            .await
            .int_err()?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }

    /// Manually triggers a transform flow of a derivative dataset
    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_transform_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn trigger_transform_flow(&self, ctx: &Context<'_>) -> Result<TriggerFlowResult> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        if let Err(e) = ensure_flow_applied_to_expected_dataset_kind(
            resolved_dataset,
            odf::DatasetKind::Derivative,
        ) {
            return Ok(TriggerFlowResult::IncompatibleDatasetKind(e));
        }

        if let Some(e) = ensure_flow_preconditions(
            ctx,
            self.dataset_request_state,
            DatasetFlowType::ExecuteTransform,
            None,
        )
        .await?
        {
            return Ok(TriggerFlowResult::PreconditionsNotMet(e));
        }

        let flow_run_service = from_catalog_n!(ctx, dyn fs::FlowRunService);
        let logged_account = utils::get_logged_account(ctx);
        let dataset_id = self.dataset_request_state.dataset_id();

        let flow_binding = afs::transform_dataset_binding(dataset_id);

        let flow_state = flow_run_service
            .run_flow_manually(
                utils::system_time_now(ctx),
                &flow_binding,
                logged_account.account_handle.did,
                None,
            )
            .await
            .int_err()?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }

    /// Manually triggers a compaction flow of a root dataset
    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_compaction_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn trigger_compaction_flow(
        &self,
        ctx: &Context<'_>,
        compaction_config_input: Option<FlowConfigCompactionInput>,
    ) -> Result<TriggerFlowResult> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        if let Err(e) =
            ensure_flow_applied_to_expected_dataset_kind(resolved_dataset, odf::DatasetKind::Root)
        {
            return Ok(TriggerFlowResult::IncompatibleDatasetKind(e));
        }

        if let Some(e) = ensure_flow_preconditions(
            ctx,
            self.dataset_request_state,
            DatasetFlowType::HardCompaction,
            None,
        )
        .await?
        {
            return Ok(TriggerFlowResult::PreconditionsNotMet(e));
        }

        let flow_run_service = from_catalog_n!(ctx, dyn fs::FlowRunService);
        let logged_account = utils::get_logged_account(ctx);
        let dataset_handle = self.dataset_request_state.dataset_handle();

        let maybe_compact_config_rule =
            if let Some(compaction_config_input) = compaction_config_input {
                match afs::FlowConfigRuleCompact::try_from(compaction_config_input) {
                    Ok(compact_config_rule) => Some(compact_config_rule),
                    Err(e) => {
                        return Ok(TriggerFlowResult::InvalidRunConfigurations(
                            FlowInvalidRunConfigurations { error: e },
                        ));
                    }
                }
            } else {
                None
            };

        let maybe_forced_configuration_rule =
            maybe_compact_config_rule.map(afs::FlowConfigRuleCompact::into_flow_config);

        let flow_binding = afs::compaction_dataset_binding(&dataset_handle.id);

        let flow_state = flow_run_service
            .run_flow_manually(
                utils::system_time_now(ctx),
                &flow_binding,
                logged_account.account_handle.did,
                maybe_forced_configuration_rule,
            )
            .await
            .int_err()?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }

    /// Manually triggers a flow that resets the dataset head to an earlier block
    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_reset_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn trigger_reset_flow(
        &self,
        ctx: &Context<'_>,
        reset_config_input: Option<FlowConfigResetInput>,
    ) -> Result<TriggerFlowResult> {
        // No constraints on dataset kind for reset flows

        if let Some(e) = ensure_flow_preconditions(
            ctx,
            self.dataset_request_state,
            DatasetFlowType::Reset,
            reset_config_input.as_ref(),
        )
        .await?
        {
            return Ok(TriggerFlowResult::PreconditionsNotMet(e));
        }

        // TODO: for some datasets launching manually might not be an option:
        //   i.e., root datasets with push sources require input data to arrive

        let (flow_run_service, dataset_registry) = from_catalog_n!(
            ctx,
            dyn fs::FlowRunService,
            dyn kamu_datasets::DatasetRegistry
        );

        let logged_account = utils::get_logged_account(ctx);
        let dataset_handle = self.dataset_request_state.dataset_handle();
        let resolved_dataset = dataset_registry.get_dataset_by_handle(dataset_handle).await;

        // Assume unwrap safe such as we have checked this existence during
        // validation step
        use odf::dataset::MetadataChainExt;
        let current_head_hash = match resolved_dataset
            .as_metadata_chain()
            .try_get_ref(&odf::BlockRef::Head)
            .await
        {
            Ok(head) => head,
            Err(e) => {
                return Err(e.into());
            }
        };

        let reset_config_rule = if let Some(reset_config_input) = reset_config_input {
            let old_head_hash = if reset_config_input.old_head_hash.is_some() {
                reset_config_input.old_head_hash.clone().map(Into::into)
            } else {
                current_head_hash
            };
            afs::FlowConfigRuleReset {
                new_head_hash: reset_config_input.new_head_hash().map(Into::into),
                old_head_hash,
            }
        } else {
            afs::FlowConfigRuleReset {
                new_head_hash: None,
                old_head_hash: current_head_hash,
            }
        };
        let maybe_forced_flow_config_rule = Some(reset_config_rule.into_flow_config());

        let flow_binding = afs::reset_dataset_binding(&dataset_handle.id);

        let flow_state = flow_run_service
            .run_flow_manually(
                utils::system_time_now(ctx),
                &flow_binding,
                logged_account.account_handle.did,
                maybe_forced_flow_config_rule,
            )
            .await
            .int_err()?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }

    /// Manually triggers a flow that removes data and keeps only metadata
    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_reset_to_metadata_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn trigger_reset_to_metadata_flow(
        &self,
        ctx: &Context<'_>,
    ) -> Result<TriggerFlowResult> {
        // No constraints on dataset kind for reset to metadata flows

        if let Some(e) = ensure_flow_preconditions(
            ctx,
            self.dataset_request_state,
            DatasetFlowType::ResetToMetadata,
            None,
        )
        .await?
        {
            return Ok(TriggerFlowResult::PreconditionsNotMet(e));
        }

        let flow_run_service = from_catalog_n!(ctx, dyn fs::FlowRunService);
        let logged_account = utils::get_logged_account(ctx);

        let flow_binding =
            afs::reset_to_metadata_dataset_binding(self.dataset_request_state.dataset_id());

        let flow_state = flow_run_service
            .run_flow_manually(
                utils::system_time_now(ctx),
                &flow_binding,
                logged_account.account_handle.did,
                None,
            )
            .await
            .int_err()?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }

    /// Cancels a scheduled or running flow of this dataset
    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_cancel_flow_run, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    pub async fn cancel_flow_run(
        &self,
        ctx: &Context<'_>,
        flow_id: FlowID,
    ) -> Result<CancelFlowRunResult> {
        let dataset_handle = self.dataset_request_state.dataset_handle();

        if let Some(error) =
            check_if_flow_belongs_to_dataset(ctx, flow_id, &dataset_handle.id).await?
        {
            return Ok(match error {
                FlowInDatasetError::NotFound(e) => CancelFlowRunResult::NotFound(e),
            });
        }

        let flow_run_service = from_catalog_n!(ctx, dyn fs::FlowRunService);

        // Attempt cancelling the flow

        let flow_state = flow_run_service
            .cancel_flow_run(utils::system_time_now(ctx), flow_id.into())
            .await
            .map_err(|e| match e {
                fs::CancelFlowRunError::NotFound(_) => unreachable!("Flow checked already"),
                fs::CancelFlowRunError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(CancelFlowRunResult::Success(CancelFlowRunSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of manually triggering a flow
#[derive(Interface)]
#[graphql(field(
    name = "message",
    ty = "String",
    desc = "Human-readable outcome message"
))]
pub enum TriggerFlowResult {
    Success(TriggerFlowSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    PreconditionsNotMet(FlowPreconditionsNotMet),
    InvalidRunConfigurations(FlowInvalidRunConfigurations),
}

/// Flow was triggered
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct TriggerFlowSuccess {
    /// Flow that was scheduled to run
    pub flow: Flow,
}

#[ComplexObject]
impl TriggerFlowSuccess {
    /// Human-readable success message
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of cancelling a flow run
#[derive(Interface)]
#[graphql(field(
    name = "message",
    ty = "String",
    desc = "Human-readable outcome message"
))]
pub enum CancelFlowRunResult {
    Success(CancelFlowRunSuccess),
    NotFound(FlowNotFound),
}

/// Flow run was cancelled
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CancelFlowRunSuccess {
    /// Flow after cancellation
    pub flow: Flow,
}

#[ComplexObject]
impl CancelFlowRunSuccess {
    /// Human-readable success message
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Provided flow run configuration is invalid
#[derive(Debug, SimpleObject)]
#[graphql(complex)]
pub struct FlowInvalidRunConfigurations {
    /// Explanation of what is invalid in the configuration
    pub error: String,
}

#[ComplexObject]
impl FlowInvalidRunConfigurations {
    /// Human-readable error message
    pub async fn message(&self) -> String {
        format!("Invalid flow configuration provided: '{}'", self.error)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
