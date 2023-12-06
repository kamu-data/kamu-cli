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
use opendatafabric::{AccountID, AccountName};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub type SystemFlowState = FlowState<SystemFlowStrategy>;
pub type SystemFlowEvent = FlowEvent<SystemFlowStrategy>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SystemFlow(Aggregate<SystemFlowState, (dyn SystemFlowEventStore + 'static)>);

impl SystemFlow {
    /// Creates a system flow
    pub fn new(
        now: DateTime<Utc>,
        flow_id: SystemFlowID,
        flow_type: SystemFlowType,
        trigger: FlowTrigger, // TODO: generalize triggers
    ) -> Self {
        Self(
            Aggregate::new(
                flow_id,
                FlowEventInitiated::<SystemFlowStrategy> {
                    event_time: now,
                    flow_id,
                    flow_key: SystemFlowKey::new(flow_type),
                    trigger,
                },
            )
            .unwrap(),
        )
    }

    /// Define start condition for the history
    pub fn define_start_condition(
        &mut self,
        now: DateTime<Utc>,
        start_condition: FlowStartCondition,
    ) -> Result<(), ProjectionError<SystemFlowState>> {
        let event = FlowEventStartConditionDefined::<SystemFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id.clone(),
            start_condition,
        };
        self.apply(event)
    }

    /// Activate at time
    pub fn activate_at_time(
        &mut self,
        now: DateTime<Utc>,
        activate_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError<SystemFlowState>> {
        let event = FlowEventQueued::<SystemFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id.clone(),
            activate_at,
        };
        self.apply(event)
    }

    /// Extra trigger
    pub fn add_trigger(
        &mut self,
        now: DateTime<Utc>,
        trigger: FlowTrigger,
    ) -> Result<(), ProjectionError<SystemFlowState>> {
        let event = FlowEventTriggerAdded::<SystemFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id.clone(),
            trigger,
        };
        self.apply(event)
    }

    /// Attaches a scheduled task
    pub fn on_task_scheduled(
        &mut self,
        now: DateTime<Utc>,
        task_id: TaskID,
    ) -> Result<(), ProjectionError<SystemFlowState>> {
        let event = FlowEventTaskScheduled::<SystemFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id.clone(),
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
    ) -> Result<(), ProjectionError<SystemFlowState>> {
        let event = FlowEventTaskFinished::<SystemFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id.clone(),
            task_id,
            task_outcome,
        };
        self.apply(event)
    }

    /// Checks if flow may be cancelled
    pub fn can_cancel(&mut self) -> bool {
        !self.outcome.is_some() && self.task_ids.is_empty() && self.cancelled_at.is_none()
    }

    /// Cancel flow before task started
    pub fn cancel(
        &mut self,
        now: DateTime<Utc>,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<(), ProjectionError<SystemFlowState>> {
        let event = FlowEventCancelled::<SystemFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id.clone(),
            by_account_id,
            by_account_name,
        };
        self.apply(event)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
