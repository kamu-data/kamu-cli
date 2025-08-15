// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use {kamu_adapter_flow_dataset as afs, kamu_flow_system as fs};

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

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetFlowRunsMut<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_ingest_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn trigger_ingest_flow(
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
                Utc::now(),
                &flow_binding,
                logged_account.account_id,
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

    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_transform_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn trigger_transform_flow(&self, ctx: &Context<'_>) -> Result<TriggerFlowResult> {
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
            .run_flow_manually(Utc::now(), &flow_binding, logged_account.account_id, None)
            .await
            .int_err()?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }

    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_compaction_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn trigger_compaction_flow(
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
                Utc::now(),
                &flow_binding,
                logged_account.account_id,
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

    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_reset_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn trigger_reset_flow(
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

        let flow_run_service = from_catalog_n!(ctx, dyn fs::FlowRunService);
        let logged_account = utils::get_logged_account(ctx);
        let dataset_handle = self.dataset_request_state.dataset_handle();

        let dataset_registry = from_catalog_n!(ctx, dyn kamu_core::DatasetRegistry);
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
                Utc::now(),
                &flow_binding,
                logged_account.account_id,
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

    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_trigger_reset_to_metadata_flow, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn trigger_reset_to_metadata_flow(&self, ctx: &Context<'_>) -> Result<TriggerFlowResult> {
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
            .run_flow_manually(Utc::now(), &flow_binding, logged_account.account_id, None)
            .await
            .int_err()?;

        Ok(TriggerFlowResult::Success(TriggerFlowSuccess {
            flow: Flow::build_batch(vec![flow_state], ctx)
                .await?
                .pop()
                .unwrap(),
        }))
    }

    #[tracing::instrument(level = "info", name = DatasetFlowRunsMut_cancel_scheduled_tasks, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn cancel_scheduled_tasks(
        &self,
        ctx: &Context<'_>,
        flow_id: FlowID,
    ) -> Result<CancelScheduledTasksResult> {
        let dataset_handle = self.dataset_request_state.dataset_handle();

        if let Some(error) =
            check_if_flow_belongs_to_dataset(ctx, flow_id, &dataset_handle.id).await?
        {
            return Ok(match error {
                FlowInDatasetError::NotFound(e) => CancelScheduledTasksResult::NotFound(e),
            });
        }

        // Attempt cancelling scheduled tasks
        let flow_run_service = from_catalog_n!(ctx, dyn fs::FlowRunService);
        let flow_state = flow_run_service
            .cancel_scheduled_tasks(flow_id.into())
            .await
            .map_err(|e| match e {
                fs::CancelScheduledTasksError::NotFound(_) => unreachable!("Flow checked already"),
                fs::CancelScheduledTasksError::Internal(e) => GqlError::Internal(e),
            })?;

        // Pause flow triggers regardless of current state.
        // Duplicate requests are auto-ignored.
        let flow_trigger_service = from_catalog_n!(ctx, dyn fs::FlowTriggerService);
        flow_trigger_service
            .pause_flow_trigger(Utc::now(), &flow_state.flow_binding)
            .await?;

        Ok(CancelScheduledTasksResult::Success(
            CancelScheduledTasksSuccess {
                flow: Flow::build_batch(vec![flow_state], ctx)
                    .await?
                    .pop()
                    .unwrap(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum TriggerFlowResult {
    Success(TriggerFlowSuccess),
    IncompatibleDatasetKind(FlowIncompatibleDatasetKind),
    PreconditionsNotMet(FlowPreconditionsNotMet),
    InvalidRunConfigurations(FlowInvalidRunConfigurations),
}

#[derive(SimpleObject)]
#[graphql(complex)]
struct TriggerFlowSuccess {
    pub flow: Flow,
}

#[ComplexObject]
impl TriggerFlowSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
enum CancelScheduledTasksResult {
    Success(CancelScheduledTasksSuccess),
    NotFound(FlowNotFound),
}

#[derive(SimpleObject)]
#[graphql(complex)]
struct CancelScheduledTasksSuccess {
    pub flow: Flow,
}

#[ComplexObject]
impl CancelScheduledTasksSuccess {
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, SimpleObject)]
#[graphql(complex)]
pub struct FlowInvalidRunConfigurations {
    pub error: String,
}

#[ComplexObject]
impl FlowInvalidRunConfigurations {
    pub async fn message(&self) -> String {
        format!("Invalid flow configuration provided: '{}'", self.error)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
