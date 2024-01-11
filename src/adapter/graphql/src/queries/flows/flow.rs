// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::PollingIngestService;
use {kamu_flow_system as fs, kamu_task_system as ts};

use crate::prelude::*;
use crate::queries::{Account, Task};

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Flow {
    flow_state: fs::FlowState,
}

#[Object]
impl Flow {
    #[graphql(skip)]
    pub fn new(flow_state: fs::FlowState) -> Self {
        Self { flow_state }
    }

    /// Unique identifier of the flow
    async fn flow_id(&self) -> FlowID {
        self.flow_state.flow_id.into()
    }

    /// Description of key flow parameters
    async fn description(&self, ctx: &Context<'_>) -> Result<FlowDescription> {
        Ok(match &self.flow_state.flow_key {
            fs::FlowKey::Dataset(fk_dataset) => {
                FlowDescription::Dataset(self.dataset_flow_description(ctx, fk_dataset).await?)
            }
            fs::FlowKey::System(fk_systen) => {
                FlowDescription::System(self.system_flow_description(fk_systen))
            }
        })
    }

    #[graphql(skip)]
    async fn dataset_flow_description(
        &self,
        ctx: &Context<'_>,
        dataset_key: &fs::FlowKeyDataset,
    ) -> Result<FlowDescriptionDataset> {
        Ok(match dataset_key.flow_type {
            fs::DatasetFlowType::Ingest => {
                let polling_ingest_svc = from_catalog::<dyn PollingIngestService>(ctx).unwrap();

                let maybe_polling_source = polling_ingest_svc
                    .get_active_polling_source(&dataset_key.dataset_id.as_local_ref())
                    .await
                    .int_err()?;

                if maybe_polling_source.is_some() {
                    FlowDescriptionDataset::PollingIngest(FlowDescriptionDatasetPollingIngest {
                        dataset_id: dataset_key.dataset_id.clone().into(),
                        ingested_records_count: None, // TODO
                    })
                } else {
                    let source_name = self.flow_state.primary_trigger.push_source_name();
                    FlowDescriptionDataset::PushIngest(FlowDescriptionDatasetPushIngest {
                        dataset_id: dataset_key.dataset_id.clone().into(),
                        source_name,
                        input_records_count: 0,       // TODO
                        ingested_records_count: None, // TODO
                    })
                }
            }
            fs::DatasetFlowType::ExecuteQuery => {
                FlowDescriptionDataset::ExecuteQuery(FlowDescriptionDatasetExecuteQuery {
                    dataset_id: dataset_key.dataset_id.clone().into(),
                    transformed_records_count: None, // TODO
                })
            }
            fs::DatasetFlowType::Compaction => {
                FlowDescriptionDataset::Compaction(FlowDescriptionDatasetCompaction {
                    dataset_id: dataset_key.dataset_id.clone().into(),
                    original_blocks_count: 0,     // TODO
                    resulting_blocks_count: None, // TODO
                })
            }
        })
    }

    #[graphql(skip)]
    fn system_flow_description(&self, system_key: &fs::FlowKeySystem) -> FlowDescriptionSystem {
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
    async fn outcome(&self) -> Option<FlowOutcome> {
        self.flow_state.outcome.map(|o| o.into())
    }

    /// Timing records associated with the flow lifecycle
    async fn timing(&self) -> FlowTimingRecords {
        self.flow_state.timing.into()
    }

    /// Associated tasks
    async fn tasks(&self, ctx: &Context<'_>) -> Result<Vec<Task>> {
        let task_scheduler = from_catalog::<dyn ts::TaskScheduler>(ctx).unwrap();

        let mut tasks = Vec::new();
        for task_id in &self.flow_state.task_ids {
            let ts_task = task_scheduler.get_task(*task_id).await.int_err()?;
            tasks.push(Task::new(ts_task));
        }
        Ok(tasks)
    }

    /// A user, who initiated the flow run. None for system-initiated flows
    async fn initiator(&self) -> Option<Account> {
        self.flow_state
            .primary_trigger
            .initiator_account_name()
            .map(|initiator| Account::from_account_name(initiator.clone()))
    }

    /// Primary flow trigger
    async fn primary_trigger(&self) -> FlowTrigger {
        self.flow_state.primary_trigger.clone().into()
    }

    /// Start condition
    async fn start_condition(&self) -> Option<FlowStartCondition> {
        self.flow_state.start_condition.map(|sc| sc.into())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Union, Clone, Eq, PartialEq)]
pub enum FlowDescription {
    #[graphql(flatten)]
    Dataset(FlowDescriptionDataset),
    #[graphql(flatten)]
    System(FlowDescriptionSystem),
}

#[derive(Union, Clone, PartialEq, Eq)]
pub enum FlowDescriptionSystem {
    GC(FlowDescriptionSystemGC),
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowDescriptionSystemGC {
    dummy: bool,
}

#[derive(Union, Clone, PartialEq, Eq)]
pub enum FlowDescriptionDataset {
    PollingIngest(FlowDescriptionDatasetPollingIngest),
    PushIngest(FlowDescriptionDatasetPushIngest),
    ExecuteQuery(FlowDescriptionDatasetExecuteQuery),
    Compaction(FlowDescriptionDatasetCompaction),
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowDescriptionDatasetPollingIngest {
    dataset_id: DatasetID,
    ingested_records_count: Option<u64>,
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowDescriptionDatasetPushIngest {
    dataset_id: DatasetID,
    source_name: Option<String>,
    input_records_count: u64,
    ingested_records_count: Option<u64>,
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowDescriptionDatasetExecuteQuery {
    dataset_id: DatasetID,
    transformed_records_count: Option<u64>,
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowDescriptionDatasetCompaction {
    dataset_id: DatasetID,
    original_blocks_count: u64,
    resulting_blocks_count: Option<u64>,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Union, Clone, Eq, PartialEq)]
pub enum FlowTrigger {
    Manual(FlowTriggerManual),
    AutoPolling(FlowTriggerAutoPolling),
    Push(FlowTriggerPush),
    InputDatasetFlow(FlowTriggerInputDatasetFlow),
}

impl From<fs::FlowTrigger> for FlowTrigger {
    fn from(value: fs::FlowTrigger) -> Self {
        match value {
            fs::FlowTrigger::Manual(manual) => Self::Manual(manual.into()),
            fs::FlowTrigger::AutoPolling(auto_polling) => Self::AutoPolling(auto_polling.into()),
            fs::FlowTrigger::Push(push) => Self::Push(push.into()),
            fs::FlowTrigger::InputDatasetFlow(input) => Self::InputDatasetFlow(input.into()),
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowTriggerManual {
    pub initiator: Account,
}

impl From<fs::FlowTriggerManual> for FlowTriggerManual {
    fn from(value: fs::FlowTriggerManual) -> Self {
        Self {
            initiator: Account::from_account_name(value.initiator_account_name),
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowTriggerAutoPolling {
    dummy: bool,
}

impl From<fs::FlowTriggerAutoPolling> for FlowTriggerAutoPolling {
    fn from(_: fs::FlowTriggerAutoPolling) -> Self {
        Self { dummy: true }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowTriggerPush {
    dummy: bool,
}

impl From<fs::FlowTriggerPush> for FlowTriggerPush {
    fn from(_: fs::FlowTriggerPush) -> Self {
        Self { dummy: true }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowTriggerInputDatasetFlow {
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
    pub flow_id: FlowID,
}

impl From<fs::FlowTriggerInputDatasetFlow> for FlowTriggerInputDatasetFlow {
    fn from(value: fs::FlowTriggerInputDatasetFlow) -> Self {
        Self {
            dataset_id: value.dataset_id.into(),
            flow_type: value.flow_type.into(),
            flow_id: value.flow_id.into(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Clone, Eq, PartialEq)]
pub enum FlowStartCondition {
    Throttling(FlowStartConditionThrottling),
    Batching(FlowStartConditionBatching),
}

impl From<fs::FlowStartCondition> for FlowStartCondition {
    fn from(value: fs::FlowStartCondition) -> Self {
        match value {
            fs::FlowStartCondition::Throttling(t) => Self::Throttling(t.into()),
            fs::FlowStartCondition::Batching(b) => Self::Batching(b.into()),
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowStartConditionThrottling {
    pub interval_sec: i64,
}

impl From<fs::FlowStartConditionThrottling> for FlowStartConditionThrottling {
    fn from(value: fs::FlowStartConditionThrottling) -> Self {
        Self {
            interval_sec: value.interval.num_seconds(),
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowStartConditionBatching {
    pub threshold_new_records: usize,
}

impl From<fs::FlowStartConditionBatching> for FlowStartConditionBatching {
    fn from(value: fs::FlowStartConditionBatching) -> Self {
        Self {
            threshold_new_records: value.threshold_new_records,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
