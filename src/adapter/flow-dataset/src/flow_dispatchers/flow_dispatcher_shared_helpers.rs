// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_adapter_task_dataset::{
    TaskResultDatasetHardCompact,
    TaskResultDatasetReset,
    TaskResultDatasetUpdate,
};
use kamu_core::{CompactionResult, PullResult};
use kamu_datasets::{DatasetEntryServiceExt, DatasetIncrementQueryService, DependencyGraphService};
use kamu_flow_system::{self as fs, FlowTriggerServiceExt};
use kamu_task_system as ts;

use crate::{FLOW_TYPE_DATASET_COMPACT, FLOW_TYPE_DATASET_TRANSFORM, FlowConfigRuleCompact};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn create_activation_cause_from_upstream_flow(
    dataset_increment_query_service: &dyn DatasetIncrementQueryService,
    success_flow_state: &fs::FlowState,
    task_result: &ts::TaskResult,
    finished_at: DateTime<Utc>,
) -> Result<Option<fs::FlowActivationCause>, InternalError> {
    let dataset_id = success_flow_state.flow_binding.get_dataset_id_or_die()?;

    struct ResultData {
        new_block_hash: odf::Multihash,
        maybe_prev_block_hash: Option<odf::Multihash>,
        blocks_added: u64,
        records_added: u64,
        was_compacted: bool,
        new_watermark: Option<DateTime<Utc>>,
    }

    let result_data: ResultData = match task_result.result_type.as_str() {
        ts::TaskResult::TASK_RESULT_EMPTY => return Ok(None),

        TaskResultDatasetReset::TYPE_ID => {
            let reset_result = TaskResultDatasetReset::from_task_result(task_result)
                .int_err()?
                .reset_result;
            ResultData {
                new_block_hash: reset_result.new_head,
                maybe_prev_block_hash: reset_result.old_head,
                blocks_added: 0,
                records_added: 0,
                was_compacted: false,
                new_watermark: None,
            }
        }

        TaskResultDatasetUpdate::TYPE_ID => {
            let update_result = TaskResultDatasetUpdate::from_task_result(task_result).int_err()?;
            match update_result.pull_result {
                PullResult::Updated { old_head, new_head } => {
                    let dataset_increment = dataset_increment_query_service
                        .get_increment_between(&dataset_id, old_head.as_ref(), &new_head)
                        .await
                        .int_err()?;

                    ResultData {
                        new_block_hash: new_head,
                        maybe_prev_block_hash: old_head,
                        blocks_added: dataset_increment.num_blocks,
                        records_added: dataset_increment.num_records,
                        was_compacted: false,
                        new_watermark: dataset_increment.updated_watermark,
                    }
                }
                PullResult::UpToDate(_) => {
                    // Dataset up to date, no new data
                    return Ok(None);
                }
            }
        }
        TaskResultDatasetHardCompact::TYPE_ID => {
            let compaction_result = TaskResultDatasetHardCompact::from_task_result(task_result)
                .int_err()?
                .compaction_result;

            match compaction_result {
                CompactionResult::NothingToDo => {
                    // Nothing to do, no data modifications
                    return Ok(None);
                }
                CompactionResult::Success {
                    old_head,
                    new_head,
                    old_num_blocks: _,
                    new_num_blocks: _,
                } => ResultData {
                    new_block_hash: new_head,
                    maybe_prev_block_hash: Some(old_head),
                    blocks_added: 0,
                    records_added: 0,
                    was_compacted: true,
                    new_watermark: None,
                },
            }
        }

        _ => {
            tracing::error!(
                "Unexpected input dataset result type: {}",
                task_result.result_type
            );
            return Err(InternalError::new(format!(
                "Unexpected input dataset result type: {}",
                task_result.result_type
            )));
        }
    };

    Ok(Some(fs::FlowActivationCause::DatasetUpdate(
        fs::FlowActivationCauseDatasetUpdate {
            activation_time: finished_at,
            dataset_id: dataset_id.clone(),
            source: fs::DatasetUpdateSource::UpstreamFlow {
                flow_type: success_flow_state.flow_binding.flow_type.clone(),
                flow_id: success_flow_state.flow_id,
            },
            new_head: result_data.new_block_hash,
            old_head_maybe: result_data.maybe_prev_block_hash,
            blocks_added: result_data.blocks_added,
            records_added: result_data.records_added,
            was_compacted: result_data.was_compacted,
            new_watermark: result_data.new_watermark,
        },
    )))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn trigger_transform_flow_for_all_downstream_datasets(
    dependency_graph_service: &dyn DependencyGraphService,
    flow_trigger_service: &dyn fs::FlowTriggerService,
    flow_run_service: &dyn fs::FlowRunService,
    flow_binding: &fs::FlowBinding,
    activation_cause: fs::FlowActivationCause,
) -> Result<(), InternalError> {
    let dataset_id = flow_binding.get_dataset_id_or_die()?;
    let downstream_dataset_ids =
        fetch_downstream_dataset_ids(dependency_graph_service, &dataset_id).await;

    for downstream_dataset_id in downstream_dataset_ids {
        let downstream_binding = fs::FlowBinding::for_dataset(
            downstream_dataset_id.clone(),
            FLOW_TYPE_DATASET_TRANSFORM,
        );
        if let Some(batching_rule) = flow_trigger_service
            .try_get_flow_batching_rule(&downstream_binding)
            .await
            .int_err()?
        {
            flow_run_service
                .run_flow_automatically(
                    &downstream_binding,
                    activation_cause.clone(),
                    Some(fs::FlowTriggerRule::Batching(batching_rule)),
                    None,
                    None,
                )
                .await
                .int_err()?;
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn trigger_hard_compaction_flow_for_own_downstream_datasets(
    dataset_entry_service: &dyn kamu_datasets::DatasetEntryService,
    dependency_graph_service: &dyn DependencyGraphService,
    flow_run_service: &dyn fs::FlowRunService,
    dataset_id: &odf::DatasetID,
    activation_cause: fs::FlowActivationCause,
) -> Result<(), InternalError> {
    let owner_account_id = dataset_entry_service
        .get_entry(dataset_id)
        .await
        .int_err()?
        .owner_id;

    for downstream_dataset_id in
        fetch_downstream_dataset_ids(dependency_graph_service, dataset_id).await
    {
        let owned = dataset_entry_service
            .is_dataset_owned_by(&downstream_dataset_id, &owner_account_id)
            .await
            .int_err()?;

        if owned {
            let downstream_binding =
                fs::FlowBinding::for_dataset(downstream_dataset_id, FLOW_TYPE_DATASET_COMPACT);
            // Trigger hard compaction
            flow_run_service
                .run_flow_automatically(
                    &downstream_binding,
                    activation_cause.clone(),
                    None,
                    Some(
                        FlowConfigRuleCompact::MetadataOnly { recursive: true }.into_flow_config(),
                    ),
                    None,
                )
                .await
                .int_err()?;
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn fetch_downstream_dataset_ids(
    dependency_graph_service: &dyn DependencyGraphService,
    dataset_id: &odf::DatasetID,
) -> Vec<odf::DatasetID> {
    // ToDo: extend dependency graph with possibility to fetch downstream
    // dependencies by owner
    use futures::StreamExt;
    dependency_graph_service
        .get_downstream_dependencies(dataset_id)
        .await
        .collect()
        .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
