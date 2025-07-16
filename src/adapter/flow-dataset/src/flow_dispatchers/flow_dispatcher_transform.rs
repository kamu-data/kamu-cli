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

use crate::{FLOW_TYPE_DATASET_TRANSFORM, trigger_transform_flow_for_all_downstream_datasets};

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
        _maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
        _maybe_task_run_arguments: Option<&ts::TaskRunArguments>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = flow_binding.get_dataset_id_or_die()?;

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
                let dataset_id = success_flow_state.flow_binding.get_dataset_id_or_die()?;

                let dataset_increment = self
                    .dataset_increment_query_service
                    .get_increment_between(&dataset_id, old_head.as_ref(), &new_head)
                    .await
                    .int_err()?;

                let activation_cause =
                    fs::FlowActivationCause::DatasetUpdate(fs::FlowActivationCauseDatasetUpdate {
                        activation_time: finish_time,
                        dataset_id,
                        source: fs::DatasetUpdateSource::UpstreamFlow {
                            flow_id: success_flow_state.flow_id,
                            flow_type: success_flow_state.flow_binding.flow_type.clone(),
                        },
                        new_head,
                        old_head_maybe: old_head,
                        changes: fs::DatasetChanges::NewData {
                            blocks_added: dataset_increment.num_blocks,
                            records_added: dataset_increment.num_records,
                            new_watermark: dataset_increment.updated_watermark,
                        },
                    });

                trigger_transform_flow_for_all_downstream_datasets(
                    self.dependency_graph_service.as_ref(),
                    self.flow_trigger_service.as_ref(),
                    self.flow_run_service.as_ref(),
                    &success_flow_state.flow_binding,
                    activation_cause,
                )
                .await?;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
