// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use {event_sourcing as evs, kamu_flow_system as fs, kamu_task_system as ts};

use super::{FlowStartCondition, FlowTrigger};
use crate::prelude::*;
use crate::queries::Task;
use crate::utils;

///////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "event_id", ty = "&EventID"),
    field(name = "event_time", ty = "&DateTime<Utc>")
)]
pub enum FlowEvent {
    /// Flow initiated
    Initiated(FlowEventInitiated),
    /// Start condition defined
    StartConditionDefined(FlowEventStartConditionDefined),
    /// Queued for time
    Queued(FlowEventQueued),
    /// Secondary trigger added
    TriggerAdded(FlowEventTriggerAdded),
    /// Associated task has changed status
    TaskChanged(FlowEventTaskChanged),
    /// Aborted flow (user cancellation or system factor, such as ds delete)
    Aborted(FlowEventAborted),
}

impl FlowEvent {
    pub fn new(event_id: evs::EventID, event: fs::FlowEvent) -> Self {
        match event {
            fs::FlowEvent::Initiated(e) => Self::Initiated(FlowEventInitiated::new(event_id, e)),
            fs::FlowEvent::StartConditionDefined(e) => {
                Self::StartConditionDefined(FlowEventStartConditionDefined::new(event_id, &e))
            }
            fs::FlowEvent::Queued(e) => Self::Queued(FlowEventQueued::new(event_id, &e)),
            fs::FlowEvent::TriggerAdded(e) => {
                Self::TriggerAdded(FlowEventTriggerAdded::new(event_id, e))
            }
            fs::FlowEvent::TaskScheduled(e) => Self::TaskChanged(FlowEventTaskChanged::new(
                event_id,
                e.event_time,
                e.task_id,
                TaskStatus::Queued,
            )),
            fs::FlowEvent::TaskRunning(e) => Self::TaskChanged(FlowEventTaskChanged::new(
                event_id,
                e.event_time,
                e.task_id,
                TaskStatus::Running,
            )),
            fs::FlowEvent::TaskFinished(e) => Self::TaskChanged(FlowEventTaskChanged::new(
                event_id,
                e.event_time,
                e.task_id,
                TaskStatus::Finished,
            )),
            fs::FlowEvent::Aborted(e) => Self::Aborted(FlowEventAborted::new(event_id, &e)),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FlowEventInitiated {
    event_id: EventID,
    event_time: DateTime<Utc>,
    trigger: FlowTrigger,
}

impl FlowEventInitiated {
    fn new(event_id: evs::EventID, event: fs::FlowEventInitiated) -> Self {
        Self {
            event_id: event_id.into(),
            event_time: event.event_time,
            trigger: event.trigger.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FlowEventStartConditionDefined {
    event_id: EventID,
    event_time: DateTime<Utc>,
    start_condition: FlowStartCondition,
}

impl FlowEventStartConditionDefined {
    fn new(event_id: evs::EventID, event: &fs::FlowEventStartConditionDefined) -> Self {
        Self {
            event_id: event_id.into(),
            event_time: event.event_time,
            start_condition: event.start_condition.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FlowEventQueued {
    event_id: EventID,
    event_time: DateTime<Utc>,
    activate_at: DateTime<Utc>,
}

impl FlowEventQueued {
    fn new(event_id: evs::EventID, event: &fs::FlowEventQueued) -> Self {
        Self {
            event_id: event_id.into(),
            event_time: event.event_time,
            activate_at: event.activate_at,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FlowEventTriggerAdded {
    event_id: EventID,
    event_time: DateTime<Utc>,
    trigger: FlowTrigger,
}

impl FlowEventTriggerAdded {
    fn new(event_id: evs::EventID, event: fs::FlowEventTriggerAdded) -> Self {
        Self {
            event_id: event_id.into(),
            event_time: event.event_time,
            trigger: event.trigger.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct FlowEventTaskChanged {
    event_id: EventID,
    event_time: DateTime<Utc>,
    task_id: TaskID,
    task_status: TaskStatus,
}

#[ComplexObject]
impl FlowEventTaskChanged {
    #[graphql(skip)]
    fn new(
        event_id: evs::EventID,
        event_time: DateTime<Utc>,
        task_id: ts::TaskID,
        task_status: TaskStatus,
    ) -> Self {
        Self {
            event_id: event_id.into(),
            event_time,
            task_id: task_id.into(),
            task_status,
        }
    }

    async fn task(&self, ctx: &Context<'_>) -> Result<Task> {
        let task_state = utils::get_task(ctx, self.task_id.into()).await?;
        Ok(Task::new(task_state))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FlowEventAborted {
    event_id: EventID,
    event_time: DateTime<Utc>,
}

impl FlowEventAborted {
    fn new(event_id: evs::EventID, event: &fs::FlowEventAborted) -> Self {
        Self {
            event_id: event_id.into(),
            event_time: event.event_time,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
