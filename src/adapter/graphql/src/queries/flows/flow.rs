// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use {kamu_flow_system as fs, kamu_task_system as ts};

use crate::prelude::*;
use crate::queries::Task;

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

    /// Planned activation time (at least, Queued state)
    async fn activate_at(&self) -> Option<DateTime<Utc>> {
        self.flow_state.activate_at
    }

    /// Recorded start of running (Running state seen at least once)
    async fn running_since(&self) -> Option<DateTime<Utc>> {
        self.flow_state.running_since
    }

    /// Recorded time of finish (succesfull or failed after retry) or abortion
    /// (Finished state seen at least once)
    async fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.flow_state.finished_at
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
}

///////////////////////////////////////////////////////////////////////////////
