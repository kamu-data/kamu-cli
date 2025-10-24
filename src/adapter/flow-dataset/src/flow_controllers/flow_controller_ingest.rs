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
use kamu_datasets::DatasetIncrementQueryService;
use kamu_flow_system::METADATA_TASK_FLOW_ID;
use kamu_task_system::{TaskMetadata, TaskScheduler};
use time_source::SystemTimeSource;
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    FLOW_TYPE_DATASET_INGEST,
    FlowConfigRuleIngest,
    FlowScopeDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowController)]
#[dill::meta(fs::FlowControllerMeta {
    flow_type: FLOW_TYPE_DATASET_INGEST,
})]
pub struct FlowControllerIngest {
    catalog: dill::Catalog,
    flow_sensor_dispatcher: Arc<dyn fs::FlowSensorDispatcher>,
    dataset_increment_query_service: Arc<dyn DatasetIncrementQueryService>,
    task_scheduler: Arc<dyn TaskScheduler>,
}

impl FlowControllerIngest {
    async fn schedule_follow_up(&self, flow_state: &fs::FlowState) -> Result<(), InternalError> {
        let logical_plan =
            <FlowControllerIngest as fs::FlowController>::build_task_logical_plan(self, flow_state)
                .await?;

        let dataset_id = FlowScopeDataset::new(&flow_state.flow_binding.scope).dataset_id();

        tracing::info!(
            flow_id = %flow_state.flow_id,
            dataset_id = %dataset_id,
            "schedule_follow_up: prepared logical plan for follow-up ingest"
        );

        tracing::debug!(
            flow_id = %flow_state.flow_id,
            "schedule_follow_up: enqueuing follow-up ingest via TaskScheduler"
        );

        let task_state = self
            .task_scheduler
            .create_task(
                logical_plan,
                Some(TaskMetadata::from(vec![(
                    METADATA_TASK_FLOW_ID,
                    flow_state.flow_id.to_string(),
                )])),
            )
            .await
            .int_err()?;

        let event_store = self.catalog.get_one::<dyn fs::FlowEventStore>().unwrap();
        let mut flow = fs::Flow::load(flow_state.flow_id, event_store.as_ref())
            .await
            .int_err()?;

        let time_source = self.catalog.get_one::<dyn SystemTimeSource>().unwrap();
        flow.on_task_scheduled(time_source.now(), task_state.task_id)
            .int_err()?;

        flow.save(event_store.as_ref()).await.int_err()?;

        tracing::info!(
            flow_id = %flow_state.flow_id,
            task_id = %task_state.task_id,
            "schedule_follow_up: follow-up task created"
        );

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl fs::FlowController for FlowControllerIngest {
    fn flow_type(&self) -> &'static str {
        FLOW_TYPE_DATASET_INGEST
    }

    #[tracing::instrument(name = "FlowControllerIngest::build_task_logical_plan", skip_all, fields(flow_id = %flow.flow_id))]
    async fn build_task_logical_plan(
        &self,
        flow: &fs::FlowState,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = FlowScopeDataset::new(&flow.flow_binding.scope).dataset_id();

        let mut fetch_uncacheable = false;
        if let Some(config_snapshot) = flow.config_snapshot.as_ref()
            && config_snapshot.rule_type == FlowConfigRuleIngest::TYPE_ID
        {
            let ingest_rule = FlowConfigRuleIngest::from_flow_config(config_snapshot)?;
            fetch_uncacheable = ingest_rule.fetch_uncacheable;
        }

        Ok(ats::LogicalPlanDatasetUpdate {
            dataset_id,
            fetch_uncacheable,
        }
        .into_logical_plan())
    }

    #[tracing::instrument(name = "FlowControllerIngest::propagate_success", skip_all, fields(flow_id = %success_flow_state.flow_id))]
    async fn propagate_success(
        &self,
        success_flow_state: &fs::FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<bool, InternalError> {
        let task_result_update =
            ats::TaskResultDatasetUpdate::from_task_result(task_result).int_err()?;

        let mut is_final_iteration = true;
        match task_result_update.pull_result {
            PullResult::UpToDate(_) => {
                tracing::debug!(flow_id = %success_flow_state.flow_id, "Ingest up-to-date, skipping propagation");
                return Ok(is_final_iteration);
            }
            PullResult::Updated {
                old_head,
                new_head,
                has_more,
            } => {
                let dataset_id =
                    FlowScopeDataset::new(&success_flow_state.flow_binding.scope).dataset_id();

                tracing::debug!(flow_id = %success_flow_state.flow_id, %dataset_id, "Ingested new data, propagating changes");

                is_final_iteration = !has_more;
                if has_more {
                    tracing::info!(
                        flow_id = %success_flow_state.flow_id,
                        %dataset_id,
                        "More data available after ingest, scheduling follow-up ingest iteration"
                    );
                    self.schedule_follow_up(success_flow_state).await?;
                }

                let dataset_increment = self
                    .dataset_increment_query_service
                    .get_increment_between(&dataset_id, old_head.as_ref(), &new_head)
                    .await
                    .int_err()?;

                let activation_cause = fs::FlowActivationCause::ResourceUpdate(
                    fs::FlowActivationCauseResourceUpdate {
                        activation_time: finish_time,
                        changes: fs::ResourceChanges::NewData(fs::ResourceDataChanges {
                            blocks_added: dataset_increment.num_blocks,
                            records_added: dataset_increment.num_records,
                            new_watermark: dataset_increment.updated_watermark,
                        }),
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

        Ok(is_final_iteration)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
