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

use crate::FlowConfigRuleIngest;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowDispatcher)]
pub struct FlowDispatcherTransform {}

#[async_trait::async_trait]
impl fs::FlowDispatcher for FlowDispatcherTransform {
    fn flow_type(&self) -> &'static str {
        "dev.kamu.flow.dispatcher.dataset.transform"
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
                "Expecting dataset flow binding scope for transform dispatcher",
            );
        };

        let mut fetch_uncacheable = false;
        if let Some(config_snapshot) = maybe_config_snapshot
            && config_snapshot.rule_type == FlowConfigRuleIngest::TYPE_ID
        {
            let ingest_rule = FlowConfigRuleIngest::from_flow_config(config_snapshot)?;
            fetch_uncacheable = ingest_rule.fetch_uncacheable;
        }

        Ok(ats::LogicalPlanDatasetUpdate {
            dataset_id: dataset_id.clone(),
            fetch_uncacheable,
        }
        .into_logical_plan())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
