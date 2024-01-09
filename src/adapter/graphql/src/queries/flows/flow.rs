// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

    /// Key of the flow
    async fn flow_key(&self) -> FlowKey {
        self.flow_state.flow_key.clone().into()
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
        match self.flow_state.primary_trigger.initiator_account_name() {
            Some(initiator) => Some(Account::from_account_name(initiator.clone().into())),
            None => None,
        }
    }

    /// Primary flow trigger
    async fn primary_trigger(&self) -> FlowTrigger {
        self.flow_state.primary_trigger.clone().into()
    }
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

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowTriggerAutoPolling {
    dummy: bool,
}

impl From<fs::FlowTriggerAutoPolling> for FlowTriggerAutoPolling {
    fn from(_: fs::FlowTriggerAutoPolling) -> Self {
        Self { dummy: true }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowTriggerPush {
    dummy: bool,
}

impl From<fs::FlowTriggerPush> for FlowTriggerPush {
    fn from(_: fs::FlowTriggerPush) -> Self {
        Self { dummy: true }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

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
