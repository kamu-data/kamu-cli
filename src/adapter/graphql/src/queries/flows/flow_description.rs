// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_TRANSFORM,
};
use kamu_adapter_task_dataset::{
    TaskResultDatasetHardCompact,
    TaskResultDatasetReset,
    TaskResultDatasetUpdate,
};
use kamu_core::{CompactionResult, PullResultUpToDate};
use kamu_datasets::{DatasetIncrementQueryService, GetIncrementError};
use kamu_flow_system::FLOW_TYPE_SYSTEM_GC;
use {kamu_flow_system as fs, kamu_task_system as ts};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowDescription {
    #[graphql(flatten)]
    Dataset(FlowDescriptionDataset),
    #[graphql(flatten)]
    System(FlowDescriptionSystem),
}

#[derive(Union)]
pub(crate) enum FlowDescriptionSystem {
    GC(FlowDescriptionSystemGC),
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionSystemGC {
    dummy: bool,
}

#[derive(Union)]
pub(crate) enum FlowDescriptionDataset {
    PollingIngest(FlowDescriptionDatasetPollingIngest),
    PushIngest(FlowDescriptionDatasetPushIngest),
    ExecuteTransform(FlowDescriptionDatasetExecuteTransform),
    HardCompaction(FlowDescriptionDatasetHardCompaction),
    Reset(FlowDescriptionDatasetReset),
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetPollingIngest {
    ingest_result: Option<FlowDescriptionUpdateResult>,
    polling_source: SetPollingSource,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetPushIngest {
    source_name: Option<String>,
    input_records_count: u64,
    ingest_result: Option<FlowDescriptionUpdateResult>,
    message: String,
    // TODO: SetPushSource
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetExecuteTransform {
    transform_result: Option<FlowDescriptionUpdateResult>,
    transform: SetTransform,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetHardCompaction {
    compaction_result: Option<FlowDescriptionDatasetHardCompactionResult>,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetReset {
    reset_result: Option<FlowDescriptionResetResult>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowDescriptionUpdateResult {
    UpToDate(FlowDescriptionUpdateResultUpToDate),
    Success(FlowDescriptionUpdateResultSuccess),
    Unknown(FlowDescriptionUpdateResultUnknown),
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionUpdateResultUnknown {
    message: String,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionUpdateResultUpToDate {
    /// The value indicates whether the api cache was used
    uncacheable: bool,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionUpdateResultSuccess {
    num_blocks: u64,
    num_records: u64,
    updated_watermark: Option<DateTime<Utc>>,
}

impl FlowDescriptionUpdateResult {
    async fn from_maybe_flow_outcome(
        maybe_outcome: Option<&fs::FlowOutcome>,
        dataset_id: &odf::DatasetID,
        increment_query_service: &dyn DatasetIncrementQueryService,
    ) -> Result<Option<Self>, InternalError> {
        if let Some(outcome) = maybe_outcome {
            match outcome {
                fs::FlowOutcome::Success(result) => match result.result_type.as_str() {
                    ts::TaskResult::TASK_RESULT_EMPTY
                    | TaskResultDatasetHardCompact::TYPE_ID
                    | TaskResultDatasetReset::TYPE_ID => Ok(None),

                    TaskResultDatasetUpdate::TYPE_ID => {
                        let update = TaskResultDatasetUpdate::from_task_result(result)?;
                        if let Some((old_head, new_head)) = update.try_as_increment() {
                            match increment_query_service
                                .get_increment_between(dataset_id, old_head, new_head)
                                .await
                            {
                                Ok(increment) => {
                                    Ok(Some(Self::Success(FlowDescriptionUpdateResultSuccess {
                                        num_blocks: increment.num_blocks,
                                        num_records: increment.num_records,
                                        updated_watermark: increment.updated_watermark,
                                    })))
                                }
                                Err(err) => {
                                    let unknown_message = match err {
                                        GetIncrementError::BlockNotFound(e) => format!(
                                            "Unable to fetch increment. Block is missing: {}",
                                            e.hash
                                        ),
                                        _ => "Unable to fetch increment".to_string(),
                                    };
                                    Ok(Some(Self::Unknown(FlowDescriptionUpdateResultUnknown {
                                        message: unknown_message,
                                    })))
                                }
                            }
                        } else if let Some(up_to_date_result) = update.try_as_up_to_date() {
                            match up_to_date_result {
                                PullResultUpToDate::PollingIngest(pi) => {
                                    Ok(Some(Self::UpToDate(FlowDescriptionUpdateResultUpToDate {
                                        uncacheable: pi.uncacheable,
                                    })))
                                }
                                PullResultUpToDate::PushIngest(pi) => {
                                    Ok(Some(Self::UpToDate(FlowDescriptionUpdateResultUpToDate {
                                        uncacheable: pi.uncacheable,
                                    })))
                                }
                                PullResultUpToDate::Sync | PullResultUpToDate::Transform => {
                                    Ok(Some(Self::UpToDate(FlowDescriptionUpdateResultUpToDate {
                                        uncacheable: false,
                                    })))
                                }
                            }
                        } else {
                            unreachable!()
                        }
                    }

                    _ => {
                        tracing::error!(
                            "Unexpected task result type: {} for flow outcome: {:?}",
                            result.result_type,
                            outcome
                        );
                        Ok(None)
                    }
                },
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug)]
enum FlowDescriptionDatasetHardCompactionResult {
    NothingToDo(FlowDescriptionHardCompactionNothingToDo),
    Success(FlowDescriptionHardCompactionSuccess),
}

#[derive(SimpleObject, Debug)]
struct FlowDescriptionHardCompactionSuccess {
    original_blocks_count: u64,
    resulting_blocks_count: u64,
    new_head: Multihash<'static>,
}

#[derive(SimpleObject, Debug, Default)]
#[graphql(complex)]
pub struct FlowDescriptionHardCompactionNothingToDo {
    _dummy: Option<String>,
}

#[ComplexObject]
impl FlowDescriptionHardCompactionNothingToDo {
    async fn message(&self) -> String {
        "Nothing to do".to_string()
    }
}

impl FlowDescriptionDatasetHardCompactionResult {
    fn from_maybe_flow_outcome(maybe_outcome: Option<&fs::FlowOutcome>) -> Result<Option<Self>> {
        if let Some(outcome) = maybe_outcome {
            match outcome {
                fs::FlowOutcome::Success(result) => match result.result_type.as_str() {
                    TaskResultDatasetReset::TYPE_ID | TaskResultDatasetUpdate::TYPE_ID => Ok(None),

                    ts::TaskResult::TASK_RESULT_EMPTY => Ok(Some(Self::NothingToDo(
                        FlowDescriptionHardCompactionNothingToDo::default(),
                    ))),

                    TaskResultDatasetHardCompact::TYPE_ID => {
                        let r = TaskResultDatasetHardCompact::from_task_result(result)?;
                        match r.compaction_result {
                            CompactionResult::NothingToDo => Ok(Some(Self::NothingToDo(
                                FlowDescriptionHardCompactionNothingToDo::default(),
                            ))),
                            CompactionResult::Success {
                                old_head: _,
                                ref new_head,
                                old_num_blocks,
                                new_num_blocks,
                            } => Ok(Some(Self::Success(FlowDescriptionHardCompactionSuccess {
                                original_blocks_count: old_num_blocks as u64,
                                resulting_blocks_count: new_num_blocks as u64,
                                new_head: new_head.clone().into(),
                            }))),
                        }
                    }

                    _ => {
                        tracing::error!(
                            "Unexpected task result type: {} for flow outcome: {:?}",
                            result.result_type,
                            outcome
                        );
                        Ok(None)
                    }
                },

                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
struct FlowDescriptionResetResult {
    new_head: Multihash<'static>,
}

impl FlowDescriptionResetResult {
    fn from_maybe_flow_outcome(maybe_outcome: Option<&fs::FlowOutcome>) -> Result<Option<Self>> {
        if let Some(outcome) = maybe_outcome {
            match outcome {
                fs::FlowOutcome::Success(result) => match result.result_type.as_str() {
                    ts::TaskResult::TASK_RESULT_EMPTY
                    | TaskResultDatasetHardCompact::TYPE_ID
                    | TaskResultDatasetUpdate::TYPE_ID => Ok(None),

                    TaskResultDatasetReset::TYPE_ID => {
                        let r = TaskResultDatasetReset::from_task_result(result)?;
                        Ok(Some(Self {
                            new_head: r.reset_result.new_head.clone().into(),
                        }))
                    }

                    _ => {
                        tracing::error!(
                            "Unexpected task result type: {} for flow outcome: {:?}",
                            result.result_type,
                            outcome
                        );
                        Ok(None)
                    }
                },
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowDescriptionBuilder {
    polling_sources_by_dataset_id: HashMap<odf::DatasetID, odf::metadata::SetPollingSource>,
    transforms_by_dataset_id: HashMap<odf::DatasetID, odf::metadata::SetTransform>,
}

impl FlowDescriptionBuilder {
    pub async fn prepare(
        ctx: &Context<'_>,
        flow_states: &[fs::FlowState],
    ) -> Result<Self, InternalError> {
        let unique_dataset_ids = FlowDescriptionBuilder::collect_unique_dataset_ids(flow_states);

        Ok(Self {
            polling_sources_by_dataset_id: HashMap::from_iter(
                FlowDescriptionBuilder::detect_datasets_with_polling_sources(
                    ctx,
                    &unique_dataset_ids,
                )
                .await?,
            ),
            transforms_by_dataset_id: HashMap::from_iter(
                FlowDescriptionBuilder::detect_datasets_with_transforms(ctx, &unique_dataset_ids)
                    .await?,
            ),
        })
    }

    fn collect_unique_dataset_ids(flow_states: &[fs::FlowState]) -> Vec<odf::DatasetID> {
        flow_states
            .iter()
            .filter_map(|flow_state| {
                if let fs::FlowScope::Dataset { dataset_id } = &flow_state.flow_binding.scope {
                    Some(dataset_id.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    }

    async fn detect_datasets_with_polling_sources(
        ctx: &Context<'_>,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<Vec<(odf::DatasetID, odf::metadata::SetPollingSource)>, InternalError> {
        // Locate datasets with polling sources
        let key_blocks_repository =
            from_catalog_n!(ctx, dyn kamu_datasets::DatasetKeyBlockRepository);

        // TODO: to be more precise, we should query the key block within the sequence
        // number range of the head at the flow launch moment,
        // as metadata might have already evolved by now
        let matches = key_blocks_repository
            .match_datasets_having_blocks(
                dataset_ids,
                &odf::BlockRef::Head,
                kamu_datasets::MetadataEventType::SetPollingSource,
            )
            .await?;

        let mut results = Vec::new();
        for (dataset_id, key_block) in matches {
            let metadata_block = odf::storage::deserialize_metadata_block(
                &key_block.block_hash,
                &key_block.block_payload,
            )
            .int_err()?;

            if let odf::MetadataEvent::SetPollingSource(set_polling_source) = metadata_block.event {
                results.push((dataset_id, set_polling_source));
            } else {
                panic!(
                    "Expected SetPollingSource event for dataset: {}, but found: {:?}",
                    dataset_id, metadata_block.event
                );
            }
        }

        Ok(results)
    }

    async fn detect_datasets_with_transforms(
        ctx: &Context<'_>,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<Vec<(odf::DatasetID, odf::metadata::SetTransform)>, InternalError> {
        // Locate datasets with transforms
        let key_blocks_repository =
            from_catalog_n!(ctx, dyn kamu_datasets::DatasetKeyBlockRepository);

        // TODO: to be more precise, we should query the key block within the sequence
        // number range of the head at the flow launch moment,
        // as metadata might have already evolved by now
        let matches = key_blocks_repository
            .match_datasets_having_blocks(
                dataset_ids,
                &odf::BlockRef::Head,
                kamu_datasets::MetadataEventType::SetTransform,
            )
            .await?;

        let mut results = Vec::new();
        for (dataset_id, key_block) in matches {
            let metadata_block = odf::storage::deserialize_metadata_block(
                &key_block.block_hash,
                &key_block.block_payload,
            )
            .int_err()?;

            if let odf::MetadataEvent::SetTransform(set_transform) = metadata_block.event {
                results.push((dataset_id, set_transform));
            } else {
                panic!(
                    "Expected SetTransform event for dataset: {}, but found: {:?}",
                    dataset_id, metadata_block.event
                );
            }
        }

        Ok(results)
    }

    pub async fn build(
        &mut self,
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
    ) -> Result<FlowDescription> {
        let flow_type = flow_state.flow_binding.flow_type.as_str();
        Ok(match &flow_state.flow_binding.scope {
            fs::FlowScope::Dataset { dataset_id } => FlowDescription::Dataset(
                self.dataset_flow_description(ctx, flow_state, flow_type, dataset_id)
                    .await?,
            ),
            fs::FlowScope::WebhookSubscription { .. } => {
                unimplemented!("WebhookSubscription flow description is not implemented yet")
            }
            fs::FlowScope::System => {
                FlowDescription::System(self.system_flow_description(flow_type)?)
            }
        })
    }

    fn system_flow_description(&self, flow_type: &str) -> Result<FlowDescriptionSystem> {
        match flow_type {
            FLOW_TYPE_SYSTEM_GC => Ok(FlowDescriptionSystem::GC(FlowDescriptionSystemGC {
                dummy: true,
            })),

            _ => {
                tracing::error!("Unexpected system flow type: {}", flow_type,);
                Err(GqlError::Internal(InternalError::new(format!(
                    "Unexpected system flow type: {flow_type}",
                ))))
            }
        }
    }

    async fn dataset_flow_description(
        &mut self,
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
        flow_type: &str,
        dataset_id: &odf::DatasetID,
    ) -> Result<FlowDescriptionDataset> {
        Ok(match flow_type {
            FLOW_TYPE_DATASET_INGEST => {
                let increment_query_service =
                    from_catalog_n!(ctx, dyn DatasetIncrementQueryService);
                let ingest_result = FlowDescriptionUpdateResult::from_maybe_flow_outcome(
                    flow_state.outcome.as_ref(),
                    dataset_id,
                    increment_query_service.as_ref(),
                )
                .await
                .int_err()?;

                if let Some(polling_source) = self.polling_sources_by_dataset_id.get(dataset_id) {
                    FlowDescriptionDataset::PollingIngest(FlowDescriptionDatasetPollingIngest {
                        ingest_result,
                        polling_source: polling_source.clone().into(),
                    })
                } else {
                    let source_name = flow_state.primary_trigger().push_source_name();
                    let trigger_description = flow_state
                        .primary_trigger()
                        .trigger_source_description()
                        .unwrap();
                    FlowDescriptionDataset::PushIngest(FlowDescriptionDatasetPushIngest {
                        source_name,
                        input_records_count: 0, // TODO
                        ingest_result,
                        message: trigger_description,
                    })
                }
            }

            FLOW_TYPE_DATASET_TRANSFORM => {
                let increment_query_service =
                    from_catalog_n!(ctx, dyn DatasetIncrementQueryService);

                if let Some(transform) = self.transforms_by_dataset_id.get(dataset_id) {
                    let transform_result = FlowDescriptionUpdateResult::from_maybe_flow_outcome(
                        flow_state.outcome.as_ref(),
                        dataset_id,
                        increment_query_service.as_ref(),
                    )
                    .await
                    .int_err()?;

                    FlowDescriptionDataset::ExecuteTransform(
                        FlowDescriptionDatasetExecuteTransform {
                            transform_result,
                            transform: transform.clone().into(),
                        },
                    )
                } else {
                    panic!("Expected SetTransform event for dataset: {dataset_id}, but found None",);
                }
            }

            FLOW_TYPE_DATASET_COMPACT => {
                FlowDescriptionDataset::HardCompaction(FlowDescriptionDatasetHardCompaction {
                    compaction_result:
                        FlowDescriptionDatasetHardCompactionResult::from_maybe_flow_outcome(
                            flow_state.outcome.as_ref(),
                        )?,
                })
            }

            FLOW_TYPE_DATASET_RESET => FlowDescriptionDataset::Reset(FlowDescriptionDatasetReset {
                reset_result: FlowDescriptionResetResult::from_maybe_flow_outcome(
                    flow_state.outcome.as_ref(),
                )?,
            }),

            _ => {
                tracing::error!("Unexpected flow type: {flow_type} for flow state: {flow_state:?}",);
                return Err(GqlError::Internal(InternalError::new(format!(
                    "Unexpected flow type: {flow_type} for flow state: {flow_state:?}",
                ))));
            }
        })
    }
}
