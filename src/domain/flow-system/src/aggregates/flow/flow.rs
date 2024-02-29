// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::*;
use kamu_task_system::{TaskID, TaskOutcome};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct Flow(Aggregate<FlowState, (dyn FlowEventStore + 'static)>);

impl Flow {
    /// Creates a flow
    pub fn new(
        now: DateTime<Utc>,
        flow_id: FlowID,
        flow_key: FlowKey,
        trigger: FlowTrigger,
    ) -> Self {
        Self(
            Aggregate::new(
                flow_id,
                FlowEventInitiated {
                    event_time: now,
                    flow_id,
                    flow_key,
                    trigger,
                },
            )
            .unwrap(),
        )
    }

    /// Define start condition to indicate the relevant reason of waiting
    pub fn set_relevant_start_condition(
        &mut self,
        now: DateTime<Utc>,
        start_condition: FlowStartCondition,
    ) -> Result<(), ProjectionError<FlowState>> {
        let event = FlowEventStartConditionUpdated {
            event_time: now,
            flow_id: self.flow_id,
            start_condition,
        };
        self.apply(event)
    }

    /// Activate at time
    pub fn activate_at_time(
        &mut self,
        now: DateTime<Utc>,
        activation_time: DateTime<Utc>,
    ) -> Result<(), ProjectionError<FlowState>> {
        let event = FlowEventActivationTimeDefined {
            event_time: now,
            flow_id: self.flow_id,
            activation_time,
        };
        self.apply(event)
    }

    /// Add extra trigger, if it's unique
    pub fn add_trigger_if_unique(
        &mut self,
        now: DateTime<Utc>,
        trigger: FlowTrigger,
    ) -> Result<bool, ProjectionError<FlowState>> {
        if trigger.is_unique_vs(&self.triggers) {
            let event = FlowEventTriggerAdded {
                event_time: now,
                flow_id: self.flow_id,
                trigger,
            };
            self.apply(event)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Attaches a scheduled task
    pub fn on_task_scheduled(
        &mut self,
        now: DateTime<Utc>,
        task_id: TaskID,
    ) -> Result<(), ProjectionError<FlowState>> {
        let event = FlowEventTaskScheduled {
            event_time: now,
            flow_id: self.flow_id,
            task_id,
        };
        self.apply(event)
    }

    /// Task running
    pub fn on_task_running(
        &mut self,
        now: DateTime<Utc>,
        task_id: TaskID,
    ) -> Result<(), ProjectionError<FlowState>> {
        let event = FlowEventTaskRunning {
            event_time: now,
            flow_id: self.flow_id,
            task_id,
        };
        self.apply(event)
    }

    /// Task finished
    pub fn on_task_finished(
        &mut self,
        now: DateTime<Utc>,
        task_id: TaskID,
        task_outcome: TaskOutcome,
    ) -> Result<(), ProjectionError<FlowState>> {
        let event = FlowEventTaskFinished {
            event_time: now,
            flow_id: self.flow_id,
            task_id,
            task_outcome,
        };
        self.apply(event)
    }

    /// Abort flow
    pub fn abort(&mut self, now: DateTime<Utc>) -> Result<(), ProjectionError<FlowState>> {
        let event = FlowEventAborted {
            event_time: now,
            flow_id: self.flow_id,
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
