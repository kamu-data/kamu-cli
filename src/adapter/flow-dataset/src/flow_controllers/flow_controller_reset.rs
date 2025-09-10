// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::DatasetEntryService;
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    FLOW_TYPE_DATASET_RESET,
    FlowConfigRuleReset,
    FlowScopeDataset,
    make_dataset_flow_sort_key,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowController)]
#[dill::meta(fs::FlowControllerMeta {
    flow_type: FLOW_TYPE_DATASET_RESET,
})]
pub struct FlowControllerReset {
    catalog: dill::Catalog,
    flow_sensor_dispatcher: Arc<dyn fs::FlowSensorDispatcher>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
}

#[async_trait::async_trait]
impl fs::FlowController for FlowControllerReset {
    fn flow_type(&self) -> &'static str {
        FLOW_TYPE_DATASET_RESET
    }

    #[tracing::instrument(name = "FlowControllerReset::build_task_logical_plan", skip_all, fields(flow_id = %flow.flow_id))]
    async fn build_task_logical_plan(
        &self,
        flow: &fs::FlowState,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = FlowScopeDataset::new(&flow.flow_binding.scope).dataset_id();

        if let Some(config_snapshot) = flow.config_snapshot.as_ref()
            && config_snapshot.rule_type == FlowConfigRuleReset::TYPE_ID
        {
            let reset_rule = FlowConfigRuleReset::from_flow_config(config_snapshot)?;
            Ok(ats::LogicalPlanDatasetReset {
                dataset_id,
                new_head_hash: reset_rule.new_head_hash,
                old_head_hash: reset_rule.old_head_hash,
            }
            .into_logical_plan())
        } else {
            InternalError::bail("Reset flow cannot be called without configuration")
        }
    }

    #[tracing::instrument(name = "FlowControllerReset::propagate_success", skip_all, fields(flow_id = %success_flow_state.flow_id))]
    async fn propagate_success(
        &self,
        success_flow_state: &fs::FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let reset_result = ats::TaskResultDatasetReset::from_task_result(task_result)
            .int_err()?
            .reset_result;

        if reset_result
            .old_head
            .as_ref()
            .is_some_and(|old_head| *old_head == reset_result.new_head)
        {
            // No reset was performed, no propagation needed
            tracing::debug!(flow_id=%success_flow_state.flow_id, "No reset performed, skipping propagation");
            return Ok(());
        }

        let dataset_id = FlowScopeDataset::new(&success_flow_state.flow_binding.scope).dataset_id();

        tracing::debug!(flow_id=%success_flow_state.flow_id, %dataset_id, "Reset successful, propagating changes");

        let activation_cause =
            fs::FlowActivationCause::ResourceUpdate(fs::FlowActivationCauseResourceUpdate {
                activation_time: finish_time,
                changes: fs::ResourceChanges::Breaking,
                resource_type: DATASET_RESOURCE_TYPE.to_string(),
                details: serde_json::to_value(DatasetResourceUpdateDetails {
                    dataset_id: dataset_id.clone(),
                    source: DatasetUpdateSource::UpstreamFlow {
                        flow_type: success_flow_state.flow_binding.flow_type.clone(),
                        flow_id: success_flow_state.flow_id,
                        maybe_flow_config_snapshot: success_flow_state.config_snapshot.clone(),
                    },
                    new_head: reset_result.new_head,
                    old_head_maybe: reset_result.old_head,
                })
                .int_err()?,
            });

        self.flow_sensor_dispatcher
            .dispatch_input_flow_success(
                &self.catalog,
                &success_flow_state.flow_binding,
                activation_cause,
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn make_flow_sort_key(
        &self,
        flow_binding: &fs::FlowBinding,
    ) -> Result<String, InternalError> {
        let scope = FlowScopeDataset::new(&flow_binding.scope);
        let dataset_id = scope.dataset_id();
        make_dataset_flow_sort_key(self.dataset_entry_service.as_ref(), &dataset_id).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
