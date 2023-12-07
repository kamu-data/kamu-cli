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

pub trait Flow<TFlowStrategy: FlowStrategy + 'static> {
    /// Returns assigned flow ID
    fn flow_id(&self) -> TFlowStrategy::FlowID;

    /// Returns flow state reference
    fn as_state(&self) -> &FlowState<TFlowStrategy>;

    /// Applies an event on the flow
    fn apply_event(
        &mut self,
        event: <FlowState<TFlowStrategy> as event_sourcing::Projection>::Event,
    ) -> Result<(), ProjectionError<FlowState<TFlowStrategy>>>;

    /// Define start condition for the history
    fn define_start_condition(
        &mut self,
        now: DateTime<Utc>,
        start_condition: FlowStartCondition,
    ) -> Result<(), ProjectionError<FlowState<TFlowStrategy>>> {
        let event = FlowEventStartConditionDefined::<TFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id(),
            start_condition,
        };
        self.apply_event(event.into())
    }

    /// Activate at time
    fn activate_at_time(
        &mut self,
        now: DateTime<Utc>,
        activate_at: DateTime<Utc>,
    ) -> Result<(), ProjectionError<FlowState<TFlowStrategy>>> {
        let event = FlowEventQueued::<TFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id(),
            activate_at,
        };
        self.apply_event(event.into())
    }

    /// Extra trigger
    fn add_trigger(
        &mut self,
        now: DateTime<Utc>,
        trigger: FlowTrigger,
    ) -> Result<(), ProjectionError<FlowState<TFlowStrategy>>> {
        let event = FlowEventTriggerAdded::<TFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id(),
            trigger,
        };
        self.apply_event(event.into())
    }

    /// Attaches a scheduled task
    fn on_task_scheduled(
        &mut self,
        now: DateTime<Utc>,
        task_id: TaskID,
    ) -> Result<(), ProjectionError<FlowState<TFlowStrategy>>> {
        let event = FlowEventTaskScheduled::<TFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id(),
            task_id,
        };
        self.apply_event(event.into())
    }

    /// Task finished
    fn on_task_finished(
        &mut self,
        now: DateTime<Utc>,
        task_id: TaskID,
        task_outcome: TaskOutcome,
    ) -> Result<(), ProjectionError<FlowState<TFlowStrategy>>> {
        let event = FlowEventTaskFinished::<TFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id(),
            task_id,
            task_outcome,
        };
        self.apply_event(event.into())
    }

    /// Checks if flow may be cancelled
    fn can_cancel(&self) -> bool {
        self.as_state().can_cancel()
    }

    /// Cancel flow before task started
    fn cancel(
        &mut self,
        now: DateTime<Utc>,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<(), ProjectionError<FlowState<TFlowStrategy>>> {
        let event = FlowEventCancelled::<TFlowStrategy> {
            event_time: now,
            flow_id: self.flow_id(),
            by_account_id,
            by_account_name,
        };
        self.apply_event(event.into())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
