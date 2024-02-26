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
use futures::StreamExt;
use kamu_core::{DatasetChangesService, DependencyGraphService, InternalError, ResultIntoInternal};
use kamu_flow_system::{
    BatchingRule,
    DatasetFlowType,
    FlowID,
    FlowKey,
    FlowKeyDataset,
    FlowResult,
    FlowState,
    FlowTrigger,
    FlowTriggerInputDatasetFlow,
};

use super::{BatchingRuleEvaluation, FlowServiceCallbacksFacade, FlowTypeSupportStrategy};

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct DatasetUpdateStrategy {
    dataset_changes_service: Arc<dyn DatasetChangesService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetUpdateStrategy {
    pub fn new(
        dataset_changes_service: Arc<dyn DatasetChangesService>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
    ) -> Self {
        Self {
            dataset_changes_service,
            dependency_graph_service,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowTypeSupportStrategy for DatasetUpdateStrategy {
    async fn on_flow_success(
        &self,
        callbacks_facade: &dyn FlowServiceCallbacksFacade,
        success_time: DateTime<Utc>,
        flow_id: FlowID,
        flow_key: &FlowKey,
        flow_result: &FlowResult,
    ) -> Result<(), InternalError> {
        if let FlowKey::Dataset(fk_dataset) = flow_key {
            // Extract list of downstream 1 level datasets
            let dependent_dataset_ids: Vec<_> = self
                .dependency_graph_service
                .get_downstream_dependencies(&fk_dataset.dataset_id)
                .await
                .int_err()?
                .collect()
                .await;

            // For each, scan if flows configurations are on
            for dependent_dataset_id in dependent_dataset_ids {
                let maybe_batching_rule = callbacks_facade.try_get_dataset_batching_rule(
                    &dependent_dataset_id,
                    DatasetFlowType::ExecuteTransform,
                );

                // When dependent flow batching rule is enabled, schedule dependent update
                if let Some(batching_rule) = maybe_batching_rule {
                    let dependent_flow_key = FlowKeyDataset::new(
                        dependent_dataset_id.clone(),
                        DatasetFlowType::ExecuteTransform,
                    )
                    .into();

                    let trigger = FlowTrigger::InputDatasetFlow(FlowTriggerInputDatasetFlow {
                        dataset_id: fk_dataset.dataset_id.clone(),
                        flow_type: fk_dataset.flow_type,
                        flow_id,
                        flow_result: flow_result.clone(),
                    });

                    callbacks_facade
                        .trigger_flow(
                            success_time,
                            &dependent_flow_key,
                            trigger,
                            Some(&batching_rule),
                        )
                        .await
                        .int_err()?;
                }
            }

            Ok(())
        } else {
            unreachable!("Not expecting other types of flow keys than dataset");
        }
    }

    async fn evaluate_batching_rule(
        &self,
        evaluation_time: DateTime<Utc>,
        flow_state: &FlowState,
        batching_rule: &BatchingRule,
    ) -> Result<BatchingRuleEvaluation, InternalError> {
        // TODO: it's likely assumed the accumulation is per each input separately, but
        // for now count overall number
        let mut accumulated_records_count = 0;
        let mut watermark_modified = false;

        // Scan each accumulated trigger to decide
        for trigger in &flow_state.triggers {
            if let FlowTrigger::InputDatasetFlow(trigger) = trigger {
                match &trigger.flow_result {
                    FlowResult::Empty => {}
                    FlowResult::DatasetUpdate(update) => {
                        // Compute increment since the first trigger by this dataset.
                        // Note: there might have been multiple updates since that time.
                        // We are only recording the first trigger of particular dataset.
                        let increment = self
                            .dataset_changes_service
                            .get_increment_since(&trigger.dataset_id, update.old_head.as_ref())
                            .await
                            .int_err()?;

                        accumulated_records_count += increment.num_records;
                        watermark_modified |= increment.updated_watermark.is_some();
                    }
                }
            }
        }

        // The timeout for batching will happen at:
        let batching_deadline =
            flow_state.timing.created_at + *batching_rule.max_batching_interval();

        // The condition is satisfied if
        //   - we crossed the number of new records threshold
        //   - or waited long enough, assuming
        //      - there is at least some change of the inputs
        //      - watmermark got touched
        let satisfied = (accumulated_records_count > 0 || watermark_modified)
            && (accumulated_records_count >= batching_rule.min_records_to_await()
                || evaluation_time >= batching_deadline);

        Ok(BatchingRuleEvaluation {
            batching_deadline,
            accumulated_records_count,
            watermark_modified,
            satisfied,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
