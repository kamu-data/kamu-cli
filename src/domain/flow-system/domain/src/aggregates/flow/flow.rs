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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct Flow(Aggregate<FlowState, (dyn FlowEventStore + 'static)>);

impl Flow {
    /// Creates a flow
    pub fn new(
        now: DateTime<Utc>,
        flow_id: FlowID,
        flow_key: FlowKey,
        trigger: FlowTriggerInstance,
        config_snapshot: Option<FlowConfigurationRule>,
    ) -> Self {
        Self(
            Aggregate::new(
                flow_id,
                FlowEventInitiated {
                    event_time: now,
                    flow_id,
                    flow_key,
                    trigger,
                    config_snapshot,
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
        if self.start_condition != Some(start_condition) {
            let event = FlowEventStartConditionUpdated {
                event_time: now,
                flow_id: self.flow_id,
                start_condition,
                last_trigger_index: self.triggers.len(),
            };
            self.apply(event)
        } else {
            Ok(())
        }
    }

    /// Define config snapshot
    pub fn modify_config_snapshot(
        &mut self,
        now: DateTime<Utc>,
        config_snapshot: FlowConfigurationRule,
    ) -> Result<(), ProjectionError<FlowState>> {
        let event = FlowConfigSnapshotModified {
            event_time: now,
            flow_id: self.flow_id,
            config_snapshot,
        };
        self.apply(event)
    }

    /// Add extra trigger, if it's unique
    pub fn add_trigger_if_unique(
        &mut self,
        now: DateTime<Utc>,
        trigger: FlowTriggerInstance,
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

    /// Indicate flow is scheduled for activation at particular time
    pub fn schedule_for_activation(
        &mut self,
        now: DateTime<Utc>,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError<FlowState>> {
        let event = FlowEventScheduledForActivation {
            event_time: now,
            flow_id: self.flow_id,
            scheduled_for_activation_at,
        };
        self.apply(event)?;
        Ok(())
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
        if !self
            .outcome
            .as_ref()
            .is_some_and(|outcome| matches!(outcome, FlowOutcome::Aborted))
        {
            let event = FlowEventAborted {
                event_time: now,
                flow_id: self.flow_id,
            };
            self.apply(event)
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
