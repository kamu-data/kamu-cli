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
use kamu_core::{DatasetChangesService, DatasetRegistry, DatasetRegistryExt, MetadataQueryService};
use kamu_flow_system::FlowResultDatasetUpdate;
use {kamu_flow_system as fs, opendatafabric as odf};

use super::{
    FlowConfigurationSnapshot,
    FlowEvent,
    FlowOutcome,
    FlowStartCondition,
    FlowTriggerType,
};
use crate::prelude::*;
use crate::queries::{Account, Task};
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct Flow {
    flow_state: Box<fs::FlowState>,
    description: FlowDescription,
}

#[Object]
impl Flow {
    #[graphql(skip)]
    pub async fn build_batch(
        flow_states: Vec<fs::FlowState>,
        ctx: &Context<'_>,
    ) -> Result<Vec<Self>> {
        let mut result: Vec<Self> = Vec::new();
        // We need this HashMap to avoid multiple queries to the same dataset polling
        // source and cover cases when dataset has no Ingest flows, so we will
        // build flow descriptions without searching of polling sources
        //
        // In addition it might be useful if we will add another entity which cause
        // duplicate requests
        let mut dataset_polling_sources: HashMap<
            opendatafabric::DatasetID,
            Option<(
                odf::Multihash,
                odf::MetadataBlockTyped<odf::SetPollingSource>,
            )>,
        > = HashMap::new();

        for flow_state in &flow_states {
            let flow_description =
                Self::build_description(ctx, flow_state, &mut dataset_polling_sources).await?;
            result.push(Self {
                flow_state: Box::new(flow_state.clone()),
                description: flow_description,
            });
        }

        Ok(result)
    }

    /// Unique identifier of the flow
    async fn flow_id(&self) -> FlowID {
        self.flow_state.flow_id.into()
    }

    #[graphql(skip)]
    async fn build_description(
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
        dataset_polling_sources_maybe: &mut HashMap<
            opendatafabric::DatasetID,
            Option<(
                odf::Multihash,
                odf::MetadataBlockTyped<odf::SetPollingSource>,
            )>,
        >,
    ) -> Result<FlowDescription> {
        Ok(match &flow_state.flow_key {
            fs::FlowKey::Dataset(fk_dataset) => FlowDescription::Dataset(
                Self::dataset_flow_description(
                    ctx,
                    flow_state,
                    fk_dataset,
                    dataset_polling_sources_maybe,
                )
                .await?,
            ),
            fs::FlowKey::System(fk_system) => {
                FlowDescription::System(Self::system_flow_description(fk_system))
            }
        })
    }

    /// Description of key flow parameters
    async fn description(&self) -> FlowDescription {
        self.description.clone()
    }

    #[graphql(skip)]
    async fn dataset_flow_description(
        ctx: &Context<'_>,
        flow_state: &fs::FlowState,
        dataset_key: &fs::FlowKeyDataset,
        dataset_polling_sources: &mut HashMap<
            opendatafabric::DatasetID,
            Option<(
                odf::Multihash,
                odf::MetadataBlockTyped<odf::SetPollingSource>,
            )>,
        >,
    ) -> Result<FlowDescriptionDataset> {
        Ok(match dataset_key.flow_type {
            fs::DatasetFlowType::Ingest => {
                let maybe_polling_source = if let Some(existing_polling_source) =
                    dataset_polling_sources.get(&dataset_key.dataset_id)
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

                    dataset_polling_sources
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

    #[graphql(skip)]
    fn system_flow_description(system_key: &fs::FlowKeySystem) -> FlowDescriptionSystem {
        match system_key.flow_type {
            fs::SystemFlowType::GC => {
                FlowDescriptionSystem::GC(FlowDescriptionSystemGC { dummy: true })
            }
        }
    }

    /// Status of the flow
    async fn status(&self) -> FlowStatus {
        self.flow_state.status().into()
    }

    /// Outcome of the flow (Finished state only)
    async fn outcome(&self, ctx: &Context<'_>) -> Result<Option<FlowOutcome>> {
        Ok(
            FlowOutcome::from_maybe_flow_outcome(&self.flow_state.outcome, ctx)
                .await
                .int_err()?,
        )
    }

    /// Timing records associated with the flow lifecycle
    async fn timing(&self) -> FlowTimingRecords {
        self.flow_state.timing.into()
    }

    /// Associated tasks
    async fn tasks(&self, ctx: &Context<'_>) -> Result<Vec<Task>> {
        let mut tasks = Vec::new();
        for task_id in &self.flow_state.task_ids {
            let ts_task = utils::get_task(ctx, *task_id).await?;
            tasks.push(Task::new(ts_task));
        }
        Ok(tasks)
    }

    /// History of flow events
    async fn history(&self, ctx: &Context<'_>) -> Result<Vec<FlowEvent>> {
        let flow_event_store = from_catalog_n!(ctx, dyn fs::FlowEventStore);

        use futures::TryStreamExt;
        let flow_events: Vec<_> = flow_event_store
            .get_events(&self.flow_state.flow_id, Default::default())
            .try_collect()
            .await
            .int_err()?;

        let mut history = Vec::new();
        for (event_id, flow_event) in flow_events {
            history.push(FlowEvent::build(event_id, flow_event, &self.flow_state, ctx).await?);
        }
        Ok(history)
    }

    /// A user, who initiated the flow run. None for system-initiated flows
    async fn initiator(&self, ctx: &Context<'_>) -> Result<Option<Account>> {
        let maybe_initiator = self.flow_state.primary_trigger().initiator_account_id();
        Ok(if let Some(initiator) = maybe_initiator {
            Some(Account::from_account_id(ctx, initiator.clone()).await?)
        } else {
            None
        })
    }

    /// Primary flow trigger
    async fn primary_trigger(&self, ctx: &Context<'_>) -> Result<FlowTriggerType, InternalError> {
        FlowTriggerType::build(self.flow_state.primary_trigger(), ctx).await
    }

    /// Start condition
    async fn start_condition(&self, ctx: &Context<'_>) -> Result<Option<FlowStartCondition>> {
        let maybe_condition =
            if let Some(start_condition) = self.flow_state.start_condition.as_ref() {
                Some(
                    FlowStartCondition::create_from_raw_flow_data(
                        start_condition,
                        &self.flow_state.triggers,
                        ctx,
                    )
                    .await
                    .int_err()?,
                )
            } else {
                None
            };

        Ok(maybe_condition)
    }

    /// Flow config snapshot
    async fn config_snapshot(&self) -> Option<FlowConfigurationSnapshot> {
        self.flow_state.config_snapshot.clone().map(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Clone)]
enum FlowDescription {
    #[graphql(flatten)]
    Dataset(FlowDescriptionDataset),
    #[graphql(flatten)]
    System(FlowDescriptionSystem),
}

#[derive(Union, Clone)]
enum FlowDescriptionSystem {
    GC(FlowDescriptionSystemGC),
}

#[derive(SimpleObject, Clone)]
struct FlowDescriptionSystemGC {
    dummy: bool,
}

#[derive(Union, Clone)]
enum FlowDescriptionDataset {
    PollingIngest(FlowDescriptionDatasetPollingIngest),
    PushIngest(FlowDescriptionDatasetPushIngest),
    ExecuteTransform(FlowDescriptionDatasetExecuteTransform),
    HardCompaction(FlowDescriptionDatasetHardCompaction),
    Reset(FlowDescriptionDatasetReset),
}

#[derive(SimpleObject, Clone)]
struct FlowDescriptionDatasetPollingIngest {
    dataset_id: DatasetID,
    ingest_result: Option<FlowDescriptionUpdateResult>,
}

#[derive(SimpleObject, Clone)]
struct FlowDescriptionDatasetPushIngest {
    dataset_id: DatasetID,
    source_name: Option<String>,
    input_records_count: u64,
    ingest_result: Option<FlowDescriptionUpdateResult>,
}

#[derive(SimpleObject, Clone)]
struct FlowDescriptionDatasetExecuteTransform {
    dataset_id: DatasetID,
    transform_result: Option<FlowDescriptionUpdateResult>,
}

#[derive(SimpleObject, Clone)]
struct FlowDescriptionDatasetHardCompaction {
    dataset_id: DatasetID,
    compaction_result: Option<FlowDescriptionDatasetHardCompactionResult>,
}

#[derive(SimpleObject, Clone)]
struct FlowDescriptionDatasetReset {
    dataset_id: DatasetID,
    reset_result: Option<FlowDescriptionResetResult>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Clone)]
enum FlowDescriptionUpdateResult {
    UpToDate(FlowDescriptionUpdateResultUpToDate),
    Success(FlowDescriptionUpdateResultSuccess),
}

#[derive(SimpleObject, Clone)]
struct FlowDescriptionUpdateResultUpToDate {
    /// The value indicates whether the api cache was used
    uncacheable: bool,
}

#[derive(SimpleObject, Clone)]
struct FlowDescriptionUpdateResultSuccess {
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
