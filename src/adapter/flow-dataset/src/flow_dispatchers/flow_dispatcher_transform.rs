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
use internal_error::InternalError;
use kamu_datasets::{DatasetIncrementQueryService, DependencyGraphService};
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowConfigRuleIngest,
    create_activation_cause_from_upstream_flow,
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
    dataset_increment_query_service: Arc<dyn DatasetIncrementQueryService>,
    flow_trigger_service: Arc<dyn fs::FlowTriggerService>,
    flow_run_service: Arc<dyn fs::FlowRunService>,
}

#[async_trait::async_trait]
impl fs::FlowDispatcher for FlowDispatcherTransform {
    async fn build_task_logical_plan(
        &self,
        flow_binding: &fs::FlowBinding,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
        _maybe_task_run_arguments: Option<&ts::TaskRunArguments>,
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
        success_flow_state: &fs::FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let maybe_activation_cause = create_activation_cause_from_upstream_flow(
            self.dataset_increment_query_service.as_ref(),
            success_flow_state,
            task_result,
            finish_time,
        )
        .await?;

        if let Some(maybe_activation_cause) = maybe_activation_cause {
            trigger_transform_flow_for_all_downstream_datasets(
                self.dependency_graph_service.as_ref(),
                self.flow_trigger_service.as_ref(),
                self.flow_run_service.as_ref(),
                &success_flow_state.flow_binding,
                maybe_activation_cause,
            )
            .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
