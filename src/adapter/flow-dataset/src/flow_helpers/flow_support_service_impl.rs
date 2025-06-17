// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_adapter_task_dataset::{
    TaskResultDatasetHardCompact,
    TaskResultDatasetReset,
    TaskResultDatasetUpdate,
};
use kamu_datasets::DatasetIncrementQueryService;
use {kamu_flow_system as fs, kamu_task_system as ts};

use crate::{FlowConfigRuleCompact, FlowConfigRuleReset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowSupportService)]
pub struct FlowSupportServiceImpl {
    dataset_increment_query_service: Arc<dyn DatasetIncrementQueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl fs::FlowSupportService for FlowSupportServiceImpl {
    async fn interpret_input_dataset_result(
        &self,
        dataset_id: &odf::DatasetID,
        input_result: &ts::TaskResult,
    ) -> Result<fs::FlowInputResultInterpretation, InternalError> {
        match input_result.result_type.as_str() {
            ts::TaskResult::TASK_RESULT_EMPTY | TaskResultDatasetReset::TYPE_ID => {
                Ok(fs::FlowInputResultInterpretation {
                    new_records_count: 0,
                    was_compacted: false,
                })
            }

            TaskResultDatasetHardCompact::TYPE_ID => Ok(fs::FlowInputResultInterpretation {
                new_records_count: 0,
                was_compacted: true,
            }),

            TaskResultDatasetUpdate::TYPE_ID => {
                let update = TaskResultDatasetUpdate::from_task_result(input_result).int_err()?;

                // Compute increment since the first trigger by this dataset.
                // Note: there might have been multiple updates since that time.
                // We are only recording the first trigger of particular dataset.
                let mut accumulated_records_count = 0;
                if let Some((old_head, _)) = update.try_as_increment() {
                    let increment = self
                        .dataset_increment_query_service
                        .get_increment_since(dataset_id, old_head)
                        .await
                        .int_err()?;

                    accumulated_records_count = increment.num_records;
                }

                Ok(fs::FlowInputResultInterpretation {
                    new_records_count: accumulated_records_count,
                    was_compacted: false,
                })
            }

            _ => {
                tracing::error!(
                    "Unexpected input dataset result type: {}",
                    input_result.result_type
                );
                Ok(fs::FlowInputResultInterpretation {
                    new_records_count: 0,
                    was_compacted: false,
                })
            }
        }
    }

    fn classify_dependent_trigger_type(
        &self,
        dataset_flow_type: fs::DatasetFlowType,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
    ) -> Result<fs::DownstreamDependencyTriggerType, InternalError> {
        use fs::{DatasetFlowType, DownstreamDependencyTriggerType};
        match dataset_flow_type {
            DatasetFlowType::Ingest | DatasetFlowType::ExecuteTransform => {
                Ok(DownstreamDependencyTriggerType::TriggerAllEnabledExecuteTransform)
            }
            DatasetFlowType::HardCompaction => {
                if let Some(config_snapshot) = &maybe_config_snapshot
                    && config_snapshot.rule_type == FlowConfigRuleCompact::TYPE_ID
                {
                    let compaction_rule = FlowConfigRuleCompact::from_flow_config(config_snapshot)?;
                    if compaction_rule.recursive() {
                        Ok(DownstreamDependencyTriggerType::TriggerOwnHardCompaction)
                    } else {
                        Ok(DownstreamDependencyTriggerType::Empty)
                    }
                } else {
                    Ok(fs::DownstreamDependencyTriggerType::TriggerAllEnabledExecuteTransform)
                }
            }
            DatasetFlowType::Reset => {
                if let Some(config_snapshot) = &maybe_config_snapshot
                    && config_snapshot.rule_type == FlowConfigRuleReset::TYPE_ID
                {
                    let reset_rule = FlowConfigRuleReset::from_flow_config(config_snapshot)?;
                    if reset_rule.recursive {
                        Ok(DownstreamDependencyTriggerType::TriggerOwnHardCompaction)
                    } else {
                        Ok(DownstreamDependencyTriggerType::Empty)
                    }
                } else {
                    Ok(DownstreamDependencyTriggerType::Empty)
                }
            }
        }
    }

    fn make_resursive_compaction_config(&self) -> fs::FlowConfigurationRule {
        FlowConfigRuleCompact::MetadataOnly { recursive: true }.into_flow_config()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
