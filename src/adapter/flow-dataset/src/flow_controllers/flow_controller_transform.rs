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
use kamu_core::PullResult;
use kamu_datasets::{DatasetIncrementQueryService, DependencyGraphService};
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    DerivedDatasetFlowSensor,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowController)]
#[dill::meta(fs::FlowControllerMeta {
    flow_type: FLOW_TYPE_DATASET_TRANSFORM,
})]
pub struct FlowControllerTransform {
    catalog: dill::Catalog,
    flow_sensor_dispatcher: Arc<dyn fs::FlowSensorDispatcher>,
    dataset_increment_query_service: Arc<dyn DatasetIncrementQueryService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

#[async_trait::async_trait]
impl fs::FlowController for FlowControllerTransform {
    fn flow_type(&self) -> &'static str {
        FLOW_TYPE_DATASET_TRANSFORM
    }

    async fn register_flow_sensor(
        &self,
        flow_binding: &fs::FlowBinding,
        activation_time: DateTime<Utc>,
        reactive_rule: fs::ReactiveRule,
    ) -> Result<(), InternalError> {
        let dataset_id = FlowScopeDataset::new(&flow_binding.scope).dataset_id();

        use futures::StreamExt;
        let upstream_dataset_ids = self
            .dependency_graph_service
            .get_upstream_dependencies(&dataset_id)
            .await
            .collect()
            .await;

        let sensor =
            DerivedDatasetFlowSensor::new(&dataset_id, upstream_dataset_ids, reactive_rule);
        self.flow_sensor_dispatcher
            .register_sensor(&self.catalog, activation_time, Arc::new(sensor))
            .await?;

        Ok(())
    }

    async fn build_task_logical_plan(
        &self,
        flow: &fs::FlowState,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = FlowScopeDataset::new(&flow.flow_binding.scope).dataset_id();

        Ok(ats::LogicalPlanDatasetUpdate {
            dataset_id,
            fetch_uncacheable: false,
        }
        .into_logical_plan())
    }

    async fn propagate_success(
        &self,
        success_flow_state: &fs::FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let task_result_update =
            ats::TaskResultDatasetUpdate::from_task_result(task_result).int_err()?;

        match task_result_update.pull_result {
            PullResult::UpToDate(_) => return Ok(()),
            PullResult::Updated { old_head, new_head } => {
                let dataset_id =
                    FlowScopeDataset::new(&success_flow_state.flow_binding.scope).dataset_id();

                let dataset_increment = self
                    .dataset_increment_query_service
                    .get_increment_between(&dataset_id, old_head.as_ref(), &new_head)
                    .await
                    .int_err()?;

                let activation_cause = fs::FlowActivationCause::ResourceUpdate(
                    fs::FlowActivationCauseResourceUpdate {
                        activation_time: finish_time,
                        changes: fs::ResourceChanges::NewData {
                            blocks_added: dataset_increment.num_blocks,
                            records_added: dataset_increment.num_records,
                            new_watermark: dataset_increment.updated_watermark,
                        },
                        resource_type: DATASET_RESOURCE_TYPE.to_string(),
                        details: serde_json::to_value(DatasetResourceUpdateDetails {
                            dataset_id,
                            source: DatasetUpdateSource::UpstreamFlow {
                                flow_id: success_flow_state.flow_id,
                                flow_type: success_flow_state.flow_binding.flow_type.clone(),
                                maybe_flow_config_snapshot: success_flow_state
                                    .config_snapshot
                                    .clone(),
                            },
                            new_head,
                            old_head_maybe: old_head,
                        })
                        .int_err()?,
                    },
                );

                self.flow_sensor_dispatcher
                    .dispatch_input_flow_success(
                        &self.catalog,
                        &success_flow_state.flow_binding,
                        activation_cause,
                    )
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
