// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use kamu_core::{DatasetChangesService, GetIncrementError};
use kamu_flow_system::{self as fs, FlowResultDatasetUpdate};

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
    dataset_id: DatasetID<'static>,
    ingest_result: Option<FlowDescriptionUpdateResult>,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetPushIngest {
    dataset_id: DatasetID<'static>,
    source_name: Option<String>,
    input_records_count: u64,
    ingest_result: Option<FlowDescriptionUpdateResult>,
    message: String,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetExecuteTransform {
    dataset_id: DatasetID<'static>,
    transform_result: Option<FlowDescriptionUpdateResult>,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetHardCompaction {
    dataset_id: DatasetID<'static>,
    compaction_result: Option<FlowDescriptionDatasetHardCompactionResult>,
}

#[derive(SimpleObject)]
pub(crate) struct FlowDescriptionDatasetReset {
    dataset_id: DatasetID<'static>,
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
        dataset_changes_service: &dyn DatasetChangesService,
    ) -> Result<Option<Self>, InternalError> {
        if let Some(outcome) = maybe_outcome {
            match outcome {
                fs::FlowOutcome::Success(result) => match result {
                    fs::FlowResult::Empty
                    | fs::FlowResult::DatasetCompact(_)
                    | fs::FlowResult::DatasetReset(_) => Ok(None),
                    fs::FlowResult::DatasetUpdate(update) => match update {
                        FlowResultDatasetUpdate::Changed(update_result) => {
                            match dataset_changes_service
                                .get_increment_between(
                                    dataset_id,
                                    update_result.old_head.as_ref(),
                                    &update_result.new_head,
                                )
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
                        }
                        FlowResultDatasetUpdate::UpToDate(up_to_date_result) => {
                            Ok(Some(Self::UpToDate(FlowDescriptionUpdateResultUpToDate {
                                uncacheable: up_to_date_result.uncacheable,
                            })))
                        }
                    },
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
    fn from_maybe_flow_outcome(maybe_outcome: Option<&fs::FlowOutcome>) -> Option<Self> {
        if let Some(outcome) = maybe_outcome {
            match outcome {
                fs::FlowOutcome::Success(result) => match result {
                    fs::FlowResult::DatasetUpdate(_) | fs::FlowResult::DatasetReset(_) => None,
                    fs::FlowResult::Empty => Some(Self::NothingToDo(
                        FlowDescriptionHardCompactionNothingToDo::default(),
                    )),
                    fs::FlowResult::DatasetCompact(compact) => {
                        Some(Self::Success(FlowDescriptionHardCompactionSuccess {
                            original_blocks_count: compact.old_num_blocks as u64,
                            resulting_blocks_count: compact.new_num_blocks as u64,
                            new_head: compact.new_head.clone().into(),
                        }))
                    }
                },
                _ => None,
            }
        } else {
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
struct FlowDescriptionResetResult {
    new_head: Multihash<'static>,
}

impl FlowDescriptionResetResult {
    fn from_maybe_flow_outcome(maybe_outcome: Option<&fs::FlowOutcome>) -> Option<Self> {
        if let Some(outcome) = maybe_outcome {
            match outcome {
                fs::FlowOutcome::Success(result) => match result {
                    fs::FlowResult::Empty
                    | fs::FlowResult::DatasetCompact(_)
                    | fs::FlowResult::DatasetUpdate(_) => None,
                    fs::FlowResult::DatasetReset(reset_result) => Some(Self {
                        new_head: reset_result.new_head.clone().into(),
                    }),
                },
                _ => None,
            }
        } else {
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowDescriptionBuilder {
    datasets_with_polling_sources: HashSet<odf::DatasetID>,
}

impl FlowDescriptionBuilder {
    pub async fn prepare(
        ctx: &Context<'_>,
        flow_states: &[fs::FlowState],
    ) -> Result<Self, InternalError> {
        Ok(Self {
            datasets_with_polling_sources: HashSet::from_iter(
                FlowDescriptionBuilder::detect_datasets_with_polling_sources(ctx, flow_states)
                    .await?,
            ),
        })
    }

    async fn detect_datasets_with_polling_sources(
        ctx: &Context<'_>,
        flow_states: &[fs::FlowState],
    ) -> Result<Vec<odf::DatasetID>, InternalError> {
        // Collect unique dataset IDs from flow states
        let dataset_ids = flow_states
            .iter()
            .filter_map(|flow_state| {
                if let kamu_flow_system::FlowKey::Dataset(fk_dataset) = &flow_state.flow_key {
                    Some(fk_dataset.dataset_id.clone())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        // Locate datasets with polling sources
        let key_blocks_repository =
            from_catalog_n!(ctx, dyn kamu_datasets::DatasetKeyBlockRepository);
        key_blocks_repository
            .filter_datasets_having_blocks(
                dataset_ids,
                &odf::BlockRef::Head,
                kamu_datasets::MetadataEventType::SetPollingSource,
            )
            .await
    }

    pub async fn build(
        &mut self,
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
    ) -> Result<FlowDescription> {
        Ok(match &flow_state.flow_key {
            fs::FlowKey::Dataset(fk_dataset) => FlowDescription::Dataset(
                self.dataset_flow_description(ctx, flow_state, fk_dataset)
                    .await?,
            ),
            fs::FlowKey::System(fk_system) => {
                FlowDescription::System(self.system_flow_description(fk_system))
            }
        })
    }

    fn system_flow_description(&self, system_key: &fs::FlowKeySystem) -> FlowDescriptionSystem {
        match system_key.flow_type {
            fs::SystemFlowType::GC => {
                FlowDescriptionSystem::GC(FlowDescriptionSystemGC { dummy: true })
            }
        }
    }

    async fn dataset_flow_description(
        &mut self,
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
        dataset_key: &fs::FlowKeyDataset,
    ) -> Result<FlowDescriptionDataset> {
        Ok(match dataset_key.flow_type {
            fs::DatasetFlowType::Ingest => {
                let dataset_changes_svc = from_catalog_n!(ctx, dyn DatasetChangesService);
                let ingest_result = FlowDescriptionUpdateResult::from_maybe_flow_outcome(
                    flow_state.outcome.as_ref(),
                    &dataset_key.dataset_id,
                    dataset_changes_svc.as_ref(),
                )
                .await
                .int_err()?;

                if self
                    .datasets_with_polling_sources
                    .contains(&dataset_key.dataset_id)
                {
                    FlowDescriptionDataset::PollingIngest(FlowDescriptionDatasetPollingIngest {
                        dataset_id: dataset_key.dataset_id.clone().into(),
                        ingest_result,
                    })
                } else {
                    let source_name = flow_state.primary_trigger().push_source_name();
                    let trigger_description = flow_state
                        .primary_trigger()
                        .trigger_source_description()
                        .unwrap();
                    FlowDescriptionDataset::PushIngest(FlowDescriptionDatasetPushIngest {
                        dataset_id: dataset_key.dataset_id.clone().into(),
                        source_name,
                        input_records_count: 0, // TODO
                        ingest_result,
                        message: trigger_description,
                    })
                }
            }
            fs::DatasetFlowType::ExecuteTransform => {
                let dataset_changes_svc = from_catalog_n!(ctx, dyn DatasetChangesService);

                FlowDescriptionDataset::ExecuteTransform(FlowDescriptionDatasetExecuteTransform {
                    dataset_id: dataset_key.dataset_id.clone().into(),
                    transform_result: FlowDescriptionUpdateResult::from_maybe_flow_outcome(
                        flow_state.outcome.as_ref(),
                        &dataset_key.dataset_id,
                        dataset_changes_svc.as_ref(),
                    )
                    .await
                    .int_err()?,
                })
            }
            fs::DatasetFlowType::HardCompaction => {
                FlowDescriptionDataset::HardCompaction(FlowDescriptionDatasetHardCompaction {
                    dataset_id: dataset_key.dataset_id.clone().into(),
                    compaction_result:
                        FlowDescriptionDatasetHardCompactionResult::from_maybe_flow_outcome(
                            flow_state.outcome.as_ref(),
                        ),
                })
            }
            fs::DatasetFlowType::Reset => {
                FlowDescriptionDataset::Reset(FlowDescriptionDatasetReset {
                    dataset_id: dataset_key.dataset_id.clone().into(),
                    reset_result: FlowDescriptionResetResult::from_maybe_flow_outcome(
                        flow_state.outcome.as_ref(),
                    ),
                })
            }
        })
    }
}
