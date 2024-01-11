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

///////////////////////////////////////////////////////////////////////////////

pub struct FlowEvent {
    event_id: evs::EventID,
    event: fs::FlowEvent,
}

#[Object]
impl FlowEvent {
    #[graphql(skip)]
    pub fn new(event_id: evs::EventID, event: fs::FlowEvent) -> Self {
        Self { event_id, event }
    }

    // TODO: create scalar type EventID
    async fn id(&self) -> String {
        self.event_id.to_string()
    }

    async fn event_time(&self) -> DateTime<Utc> {
        self.event.event_time()
    }

    async fn event_data(&self, ctx: &Context<'_>) -> Result<FlowEventData> {
        Ok(match &self.event {
            fs::FlowEvent::Initiated(e) => FlowEventData::Initiated(FlowEventInitiated {
                trigger: e.trigger.clone().into(),
            }),
            fs::FlowEvent::StartConditionDefined(e) => {
                FlowEventData::StartConditionDefined(FlowEventStartConditionDefined {
                    start_condition: e.start_condition.into(),
                })
            }
            fs::FlowEvent::Queued(e) => FlowEventData::Queued(FlowEventQueued {
                activate_at: e.activate_at,
            }),
            fs::FlowEvent::TriggerAdded(e) => FlowEventData::TriggerAdded(FlowEventTriggerAdded {
                trigger: e.trigger.clone().into(),
            }),
            fs::FlowEvent::TaskScheduled(e) => {
                FlowEventData::TaskScheduled(FlowEventTaskScheduled {
                    task: Self::resolve_task(ctx, e.task_id).await?,
                })
            }
            fs::FlowEvent::TaskRunning(e) => FlowEventData::TaskRunning(FlowEventTaskRunning {
                task: Self::resolve_task(ctx, e.task_id).await?,
            }),
            fs::FlowEvent::TaskFinished(e) => FlowEventData::TaskFinished(FlowEventTaskFinished {
                task: Self::resolve_task(ctx, e.task_id).await?,
            }),
            fs::FlowEvent::Aborted(_) => FlowEventData::Aborted(FlowEventAborted { dummy: true }),
        })
    }

    #[graphql(skip)]
    async fn resolve_task(ctx: &Context<'_>, task_id: ts::TaskID) -> Result<Task> {
        let task_scheduler = from_catalog::<dyn ts::TaskScheduler>(ctx).unwrap();
        let task_state = task_scheduler.get_task(task_id).await.int_err()?;
        Ok(Task::new(task_state))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
enum FlowEventData {
    /// Flow initiated
    Initiated(FlowEventInitiated),
    /// Start condition defined
    StartConditionDefined(FlowEventStartConditionDefined),
    /// Queued for time
    Queued(FlowEventQueued),
    /// Secondary triger added
    TriggerAdded(FlowEventTriggerAdded),
    /// Scheduled/Rescheduled a task
    TaskScheduled(FlowEventTaskScheduled),
    /// Task running
    TaskRunning(FlowEventTaskRunning),
    /// Finished task
    TaskFinished(FlowEventTaskFinished),
    /// Aborted flow (system factor, such as dataset delete)
    Aborted(FlowEventAborted),
}

#[derive(SimpleObject)]
struct FlowEventInitiated {
    trigger: FlowTrigger,
}

#[derive(SimpleObject)]
struct FlowEventStartConditionDefined {
    start_condition: FlowStartCondition,
}

#[derive(SimpleObject)]
struct FlowEventQueued {
    activate_at: DateTime<Utc>,
}

#[derive(SimpleObject)]
struct FlowEventTriggerAdded {
    trigger: FlowTrigger,
}

#[derive(SimpleObject)]
struct FlowEventTaskScheduled {
    task: Task,
}

#[derive(SimpleObject)]
struct FlowEventTaskRunning {
    task: Task,
}

#[derive(SimpleObject)]
struct FlowEventTaskFinished {
    task: Task,
}

#[derive(SimpleObject)]
struct FlowEventAborted {
    dummy: bool,
}

///////////////////////////////////////////////////////////////////////////////
