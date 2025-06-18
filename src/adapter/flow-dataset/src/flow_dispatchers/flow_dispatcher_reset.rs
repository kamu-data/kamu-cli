// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::FlowConfigRuleReset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowDispatcher)]
pub struct FlowDispatcherReset {}

#[async_trait::async_trait]
impl fs::FlowDispatcher for FlowDispatcherReset {
    fn flow_type(&self) -> &'static str {
        "dev.kamu.flow.dispatcher.dataset.reset"
    }

    fn matches(&self, binding: &fs::FlowBinding) -> bool {
        binding.flow_type == self.flow_type()
            && matches!(binding.scope, fs::FlowScope::Dataset { .. })
    }

    async fn build_task_logical_plan(
        &self,
        flow_binding: &fs::FlowBinding,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let fs::FlowScope::Dataset { dataset_id } = &flow_binding.scope else {
            return InternalError::bail(
                "Expecting dataset flow binding scope for reset dispatcher",
            );
        };

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
