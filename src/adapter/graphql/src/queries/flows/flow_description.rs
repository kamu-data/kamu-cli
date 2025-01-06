// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use kamu_core::{
    DatasetChangesService,
    DatasetRegistry,
    DatasetRegistryExt,
    MetadataQueryService,
    PollingSourceBlockInfo,
};
use kamu_flow_system::FlowResultDatasetUpdate;
use {kamu_flow_system as fs, opendatafabric as odf};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Clone)]
pub(crate) enum FlowDescription {
    #[graphql(flatten)]
    Dataset(FlowDescriptionDataset),
    #[graphql(flatten)]
    System(FlowDescriptionSystem),
}

#[derive(Union, Clone)]
pub(crate) enum FlowDescriptionSystem {
    GC(FlowDescriptionSystemGC),
}

#[derive(SimpleObject, Clone)]
pub(crate) struct FlowDescriptionSystemGC {
    dummy: bool,
}

#[derive(Union, Clone)]
pub(crate) enum FlowDescriptionDataset {
    PollingIngest(FlowDescriptionDatasetPollingIngest),
    PushIngest(FlowDescriptionDatasetPushIngest),
    ExecuteTransform(FlowDescriptionDatasetExecuteTransform),
    HardCompaction(FlowDescriptionDatasetHardCompaction),
    Reset(FlowDescriptionDatasetReset),
}

#[derive(SimpleObject, Clone)]
pub(crate) struct FlowDescriptionDatasetPollingIngest {
    dataset_id: DatasetID,
    ingest_result: Option<FlowDescriptionUpdateResult>,
}

#[derive(SimpleObject, Clone)]
pub(crate) struct FlowDescriptionDatasetPushIngest {
    dataset_id: DatasetID,
    source_name: Option<String>,
    input_records_count: u64,
    ingest_result: Option<FlowDescriptionUpdateResult>,
}

#[derive(SimpleObject, Clone)]
pub(crate) struct FlowDescriptionDatasetExecuteTransform {
    dataset_id: DatasetID,
    transform_result: Option<FlowDescriptionUpdateResult>,
}

#[derive(SimpleObject, Clone)]
pub(crate) struct FlowDescriptionDatasetHardCompaction {
    dataset_id: DatasetID,
    compaction_result: Option<FlowDescriptionDatasetHardCompactionResult>,
}

#[derive(SimpleObject, Clone)]
pub(crate) struct FlowDescriptionDatasetReset {
    dataset_id: DatasetID,
    reset_result: Option<FlowDescriptionResetResult>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Clone)]
pub(crate) enum FlowDescriptionUpdateResult {
    UpToDate(FlowDescriptionUpdateResultUpToDate),
    Success(FlowDescriptionUpdateResultSuccess),
}

#[derive(SimpleObject, Clone)]
pub(crate) struct FlowDescriptionUpdateResultUpToDate {
    /// The value indicates whether the api cache was used
    uncacheable: bool,
}

#[derive(SimpleObject, Clone)]
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
                            let increment = dataset_changes_service
                                .get_increment_between(
                                    dataset_id,
                                    update_result.old_head.as_ref(),
                                    &update_result.new_head,
                                )
                                .await
                                .int_err()?;

                            Ok(Some(Self::Success(FlowDescriptionUpdateResultSuccess {
                                num_blocks: increment.num_blocks,
                                num_records: increment.num_records,
                                updated_watermark: increment.updated_watermark,
                            })))
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

#[derive(Union, Debug, Clone)]
enum FlowDescriptionDatasetHardCompactionResult {
    NothingToDo(FlowDescriptionHardCompactionNothingToDo),
    Success(FlowDescriptionHardCompactionSuccess),
}

#[derive(SimpleObject, Debug, Clone)]
struct FlowDescriptionHardCompactionSuccess {
    original_blocks_count: u64,
    resulting_blocks_count: u64,
    new_head: Multihash,
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct FlowDescriptionHardCompactionNothingToDo {
    pub _dummy: String,
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
                        FlowDescriptionHardCompactionNothingToDo {
                            _dummy: "Nothing to do".to_string(),
                        },
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

#[derive(SimpleObject, Clone)]
struct FlowDescriptionResetResult {
    new_head: Multihash,
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
    // We need this HashMap to avoid multiple queries to the same dataset polling
    // source and cover cases when dataset has no Ingest flows, so we will
    // build flow descriptions without searching of polling sources
    //
    // In addition it might be useful if we will add another entity which cause
    // duplicate requests
    dataset_polling_sources: HashMap<opendatafabric::DatasetID, Option<PollingSourceBlockInfo>>,
}

impl FlowDescriptionBuilder {
    pub fn new() -> Self {
        Self {
            dataset_polling_sources: HashMap::new(),
        }
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
                let maybe_polling_source = if let Some(existing_polling_source) =
                    self.dataset_polling_sources.get(&dataset_key.dataset_id)
                {
                    existing_polling_source.clone()
                } else {
                    let (dataset_registry, metadata_query_service) =
                        from_catalog_n!(ctx, dyn DatasetRegistry, dyn MetadataQueryService);
                    let target = dataset_registry
                        .get_dataset_by_ref(&dataset_key.dataset_id.as_local_ref())
                        .await
                        .int_err()?;

                    let polling_source_maybe = metadata_query_service
                        .get_active_polling_source(target)
                        .await
                        .int_err()?;

                    self.dataset_polling_sources
                        .insert(dataset_key.dataset_id.clone(), polling_source_maybe.clone());
                    polling_source_maybe
                };

                let dataset_changes_svc = from_catalog_n!(ctx, dyn DatasetChangesService);
                let ingest_result = FlowDescriptionUpdateResult::from_maybe_flow_outcome(
                    flow_state.outcome.as_ref(),
                    &dataset_key.dataset_id,
                    dataset_changes_svc.as_ref(),
                )
                .await
                .int_err()?;

                if maybe_polling_source.is_some() {
                    FlowDescriptionDataset::PollingIngest(FlowDescriptionDatasetPollingIngest {
                        dataset_id: dataset_key.dataset_id.clone().into(),
                        ingest_result,
                    })
                } else {
                    let source_name = flow_state.primary_trigger().push_source_name();
                    FlowDescriptionDataset::PushIngest(FlowDescriptionDatasetPushIngest {
                        dataset_id: dataset_key.dataset_id.clone().into(),
                        source_name,
                        input_records_count: 0, // TODO
                        ingest_result,
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
