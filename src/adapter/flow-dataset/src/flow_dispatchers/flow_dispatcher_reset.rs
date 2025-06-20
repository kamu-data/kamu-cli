// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_datasets::{DatasetEntryService, DependencyGraphService};
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    FLOW_TYPE_DATASET_RESET,
    FlowConfigRuleReset,
    trigger_hard_compaction_flow_for_own_downstream_datasets,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowDispatcher)]
#[dill::meta(fs::FlowDispatcherMeta {
    flow_dispatcher_type: FLOW_TYPE_DATASET_RESET,
})]
pub struct FlowDispatcherReset {
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    flow_query_service: Arc<dyn fs::FlowQueryService>,
}

#[async_trait::async_trait]
impl fs::FlowDispatcher for FlowDispatcherReset {
    async fn build_task_logical_plan(
        &self,
        flow_binding: &fs::FlowBinding,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = flow_binding.dataset_id_or_die()?;

        if let Some(config_snapshot) = maybe_config_snapshot
            && config_snapshot.rule_type == FlowConfigRuleReset::TYPE_ID
        {
            let reset_rule = FlowConfigRuleReset::from_flow_config(config_snapshot)?;
            Ok(ats::LogicalPlanDatasetReset {
                dataset_id: dataset_id.clone(),
                new_head_hash: reset_rule.new_head_hash.clone(),
                old_head_hash: reset_rule.old_head_hash.clone(),
                recursive: reset_rule.recursive,
            }
            .into_logical_plan())
        } else {
            InternalError::bail("Reset flow cannot be called without configuration")
        }
    }

    async fn propagate_success(
        &self,
        flow_binding: &fs::FlowBinding,
        trigger_instance: fs::FlowTriggerInstance,
        maybe_config_snapshot: Option<fs::FlowConfigurationRule>,
    ) -> Result<(), InternalError> {
        let dataset_id = flow_binding.dataset_id_or_die()?;

        if let Some(config_snapshot) = &maybe_config_snapshot
            && config_snapshot.rule_type == FlowConfigRuleReset::TYPE_ID
        {
            let reset_rule = FlowConfigRuleReset::from_flow_config(config_snapshot)?;
            if reset_rule.recursive {
                return trigger_hard_compaction_flow_for_own_downstream_datasets(
                    self.dataset_entry_service.as_ref(),
                    self.dependency_graph_service.as_ref(),
                    self.flow_query_service.as_ref(),
                    &dataset_id,
                    trigger_instance,
                )
                .await;
            }
        }

        // No propagation needed
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
