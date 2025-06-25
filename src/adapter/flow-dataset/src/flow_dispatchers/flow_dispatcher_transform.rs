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
use kamu_datasets::DependencyGraphService;
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowConfigRuleIngest,
    trigger_transform_flow_for_all_downstream_datasets,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowDispatcher)]
#[dill::meta(fs::FlowDispatcherMeta {
    flow_type: FLOW_TYPE_DATASET_TRANSFORM,
})]
pub struct FlowDispatcherTransform {
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    flow_trigger_service: Arc<dyn fs::FlowTriggerService>,
    flow_run_service: Arc<dyn fs::FlowRunService>,
}

#[async_trait::async_trait]
impl fs::FlowDispatcher for FlowDispatcherTransform {
    async fn build_task_logical_plan(
        &self,
        flow_binding: &fs::FlowBinding,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = flow_binding.get_dataset_id_or_die()?;

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

    async fn propagate_success(
        &self,
        flow_binding: &fs::FlowBinding,
        trigger_instance: fs::FlowTriggerInstance,
        _: Option<fs::FlowConfigurationRule>,
    ) -> Result<(), InternalError> {
        trigger_transform_flow_for_all_downstream_datasets(
            self.dependency_graph_service.as_ref(),
            self.flow_trigger_service.as_ref(),
            self.flow_run_service.as_ref(),
            flow_binding,
            trigger_instance,
        )
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
