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

use super::{FlowConfigurationSnapshot, FlowStartCondition, FlowTriggerType};
use crate::prelude::*;
use crate::queries::Task;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "event_id", ty = "&EventID"),
    field(name = "event_time", ty = "&DateTime<Utc>")
)]
pub enum FlowEvent {
    /// Flow initiated
    Initiated(FlowEventInitiated),
    /// Start condition defined
    StartConditionUpdated(FlowEventStartConditionUpdated),
    /// Config snapshot modified
    ConfigSnapshotModified(FlowConfigSnapshotModified),
    /// Flow scheduled for activation
    ScheduledForActivation(FlowEventScheduledForActivation),
    /// Secondary trigger added
    TriggerAdded(FlowEventTriggerAdded),
    /// Associated task has changed status
    TaskChanged(FlowEventTaskChanged),
    /// Aborted flow (user cancellation or system factor, such as ds delete)
    Aborted(FlowEventAborted),
}

impl FlowEvent {
    pub async fn build(
        event_id: evs::EventID,
        event: fs::FlowEvent,
        flow_state: &fs::FlowState,
        ctx: &Context<'_>,
    ) -> Result<Self, InternalError> {
        Ok(match event {
            fs::FlowEvent::Initiated(e) => {
                Self::Initiated(FlowEventInitiated::build(event_id, e, ctx).await?)
            }
            fs::FlowEvent::StartConditionUpdated(e) => {
                let start_condition = FlowStartCondition::create_from_raw_flow_data(
                    &e.start_condition,
                    &flow_state.triggers[0..e.last_trigger_index],
                    ctx,
                )
                .await?;
                Self::StartConditionUpdated(FlowEventStartConditionUpdated::new(
                    event_id,
                    e.event_time,
                    start_condition,
                ))
            }
            fs::FlowEvent::ConfigSnapshotModified(e) => {
                Self::ConfigSnapshotModified(FlowConfigSnapshotModified::build(event_id, e))
            }
            fs::FlowEvent::TriggerAdded(e) => {
                Self::TriggerAdded(FlowEventTriggerAdded::build(event_id, e, ctx).await?)
            }
            fs::FlowEvent::ScheduledForActivation(e) => {
                Self::ScheduledForActivation(FlowEventScheduledForActivation::new(
                    event_id,
                    e.event_time,
                    e.scheduled_for_activation_at,
                ))
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
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FlowEventInitiated {
    event_id: EventID,
    event_time: DateTime<Utc>,
    trigger: FlowTriggerType,
}

impl FlowEventInitiated {
    pub(crate) async fn build(
        event_id: evs::EventID,
        event: fs::FlowEventInitiated,
        ctx: &Context<'_>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            event_id: event_id.into(),
            event_time: event.event_time,
            trigger: FlowTriggerType::build(&event.trigger, ctx).await?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FlowEventStartConditionUpdated {
    event_id: EventID,
    event_time: DateTime<Utc>,
    start_condition: FlowStartCondition,
}

impl FlowEventStartConditionUpdated {
    fn new(
        event_id: evs::EventID,
        event_time: DateTime<Utc>,
        start_condition: FlowStartCondition,
    ) -> Self {
        Self {
            event_id: event_id.into(),
            event_time,
            start_condition,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FlowEventTriggerAdded {
    event_id: EventID,
    event_time: DateTime<Utc>,
    trigger: FlowTriggerType,
}

impl FlowEventTriggerAdded {
    pub(crate) async fn build(
        event_id: evs::EventID,
        event: fs::FlowEventTriggerAdded,
        ctx: &Context<'_>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            event_id: event_id.into(),
            event_time: event.event_time,
            trigger: FlowTriggerType::build(&event.trigger, ctx).await?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct FlowConfigSnapshotModified {
    event_id: EventID,
    event_time: DateTime<Utc>,
    config_snapshot: FlowConfigurationSnapshot,
}

impl FlowConfigSnapshotModified {
    pub(crate) fn build(event_id: evs::EventID, event: fs::FlowConfigSnapshotModified) -> Self {
        Self {
            event_id: event_id.into(),
            event_time: event.event_time,
            config_snapshot: event.config_snapshot.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct FlowEventScheduledForActivation {
    event_id: EventID,
    event_time: DateTime<Utc>,
    scheduled_for_activation_at: DateTime<Utc>,
}

#[ComplexObject]
impl FlowEventScheduledForActivation {
    #[graphql(skip)]
    fn new(
        event_id: evs::EventID,
        event_time: DateTime<Utc>,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Self {
        Self {
            event_id: event_id.into(),
            event_time,
            scheduled_for_activation_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
