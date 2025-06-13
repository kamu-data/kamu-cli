// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use {kamu_flow_system as fs, kamu_task_system as ts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowTaskFactory)]
pub struct FlowTaskFactoryImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl fs::FlowTaskFactory for FlowTaskFactoryImpl {
    async fn build_task_logical_plan(
        &self,
        flow_key: &fs::FlowKey,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        match flow_key {
            fs::FlowKey::Dataset(flow_key) => match flow_key.flow_type {
                fs::DatasetFlowType::Ingest | fs::DatasetFlowType::ExecuteTransform => {
                    let mut fetch_uncacheable = false;
                    if let Some(config_snapshot) = maybe_config_snapshot
                        && let fs::FlowConfigurationRule::IngestRule(ingest_rule) = config_snapshot
                    {
                        fetch_uncacheable = ingest_rule.fetch_uncacheable;
                    }
                    Ok(ts::LogicalPlan::UpdateDataset(
                        ts::LogicalPlanUpdateDataset {
                            dataset_id: flow_key.dataset_id.clone(),
                            fetch_uncacheable,
                        },
                    ))
                }
                fs::DatasetFlowType::HardCompaction => {
                    let mut max_slice_size: Option<u64> = None;
                    let mut max_slice_records: Option<u64> = None;
                    let mut keep_metadata_only = false;

                    if let Some(config_snapshot) = maybe_config_snapshot
                        && let fs::FlowConfigurationRule::CompactionRule(compaction_rule) =
                            config_snapshot
                    {
                        max_slice_size = compaction_rule.max_slice_size();
                        max_slice_records = compaction_rule.max_slice_records();
                        keep_metadata_only =
                            matches!(compaction_rule, fs::CompactionRule::MetadataOnly(_));
                    }

                    Ok(ts::LogicalPlan::HardCompactDataset(
                        ts::LogicalPlanHardCompactDataset {
                            dataset_id: flow_key.dataset_id.clone(),
                            max_slice_size,
                            max_slice_records,
                            keep_metadata_only,
                        },
                    ))
                }
                fs::DatasetFlowType::Reset => {
                    if let Some(config_rule) = maybe_config_snapshot
                        && let fs::FlowConfigurationRule::ResetRule(reset_rule) = config_rule
                    {
                        return Ok(ts::LogicalPlan::ResetDataset(ts::LogicalPlanResetDataset {
                            dataset_id: flow_key.dataset_id.clone(),
                            new_head_hash: reset_rule.new_head_hash.clone(),
                            old_head_hash: reset_rule.old_head_hash.clone(),
                            recursive: reset_rule.recursive,
                        }));
                    }
                    InternalError::bail("Reset flow cannot be called without configuration")
                }
            },
            fs::FlowKey::System(flow_key) => {
                match flow_key.flow_type {
                    // TODO: replace on correct logical plan
                    fs::SystemFlowType::GC => Ok(ts::LogicalPlan::Probe(ts::LogicalPlanProbe {
                        dataset_id: None,
                        busy_time: Some(std::time::Duration::from_secs(20)),
                        end_with_outcome: Some(ts::TaskOutcome::Success(ts::TaskResult::Empty)),
                    })),
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
