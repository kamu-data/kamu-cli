// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use enum_variants::*;
use kamu_task_system::{TaskID, TaskOutcome};
use opendatafabric::{AccountID, AccountName};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub trait FlowStrategy: std::fmt::Debug + Clone {
    type FlowID: std::fmt::Debug + Copy + PartialEq + Eq + Send + Sync + 'static;
    type FlowKey: std::fmt::Debug + Clone + PartialEq + Eq + Send + Sync + 'static;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowEvent<TFlowStrategy: FlowStrategy> {
    /// Update initiated
    Initiated(FlowEventInitiated<TFlowStrategy>),
    /// Start condition defined
    StartConditionDefined(FlowEventStartConditionDefined<TFlowStrategy>),
    /// Queued for time
    Queued(FlowEventQueued<TFlowStrategy>),
    /// Secondary triger added
    TriggerAdded(FlowEventTriggerAdded<TFlowStrategy>),
    /// Scheduled/Rescheduled a task
    TaskScheduled(FlowEventTaskScheduled<TFlowStrategy>),
    /// Finished task
    TaskFinished(FlowEventTaskFinished<TFlowStrategy>),
    /// Cancelled update
    Cancelled(FlowEventCancelled<TFlowStrategy>),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventInitiated<TFlowStrategy: FlowStrategy> {
    pub event_time: DateTime<Utc>,
    pub flow_id: TFlowStrategy::FlowID,
    pub flow_key: TFlowStrategy::FlowKey,
    pub trigger: FlowTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventStartConditionDefined<TFlowStrategy: FlowStrategy> {
    pub event_time: DateTime<Utc>,
    pub flow_id: TFlowStrategy::FlowID,
    pub start_condition: FlowStartCondition,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventQueued<TFlowStrategy: FlowStrategy> {
    pub event_time: DateTime<Utc>,
    pub flow_id: TFlowStrategy::FlowID,
    pub activate_at: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventTriggerAdded<TFlowStrategy: FlowStrategy> {
    pub event_time: DateTime<Utc>,
    pub flow_id: TFlowStrategy::FlowID,
    pub trigger: FlowTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventTaskScheduled<TFlowStrategy: FlowStrategy> {
    pub event_time: DateTime<Utc>,
    pub flow_id: TFlowStrategy::FlowID,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventTaskFinished<TFlowStrategy: FlowStrategy> {
    pub event_time: DateTime<Utc>,
    pub flow_id: TFlowStrategy::FlowID,
    pub task_id: TaskID,
    pub task_outcome: TaskOutcome,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventCancelled<TFlowStrategy: FlowStrategy> {
    pub event_time: DateTime<Utc>,
    pub flow_id: TFlowStrategy::FlowID,
    pub by_account_id: AccountID,
    pub by_account_name: AccountName,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<TFlowStrategy: FlowStrategy> FlowEvent<TFlowStrategy> {
    pub fn flow_id(&self) -> TFlowStrategy::FlowID {
        match self {
            FlowEvent::<TFlowStrategy>::Initiated(e) => e.flow_id,
            FlowEvent::<TFlowStrategy>::StartConditionDefined(e) => e.flow_id,
            FlowEvent::<TFlowStrategy>::Queued(e) => e.flow_id,
            FlowEvent::<TFlowStrategy>::TriggerAdded(e) => e.flow_id,
            FlowEvent::<TFlowStrategy>::TaskScheduled(e) => e.flow_id,
            FlowEvent::<TFlowStrategy>::TaskFinished(e) => e.flow_id,
            FlowEvent::<TFlowStrategy>::Cancelled(e) => e.flow_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            FlowEvent::<TFlowStrategy>::Initiated(e) => &e.event_time,
            FlowEvent::<TFlowStrategy>::StartConditionDefined(e) => &e.event_time,
            FlowEvent::<TFlowStrategy>::Queued(e) => &e.event_time,
            FlowEvent::<TFlowStrategy>::TriggerAdded(e) => &e.event_time,
            FlowEvent::<TFlowStrategy>::TaskScheduled(e) => &e.event_time,
            FlowEvent::<TFlowStrategy>::TaskFinished(e) => &e.event_time,
            FlowEvent::<TFlowStrategy>::Cancelled(e) => &e.event_time,
        }
    }
}

impl_generic_enum_with_variants!(FlowEvent<TFlowStrategy: FlowStrategy>);

impl_generic_enum_variant!(FlowEvent::Initiated(FlowEventInitiated<TFlowStrategy: FlowStrategy>));
impl_generic_enum_variant!(
    FlowEvent::StartConditionDefined(FlowEventStartConditionDefined<TFlowStrategy: FlowStrategy>)
);
impl_generic_enum_variant!(FlowEvent::Queued(FlowEventQueued<TFlowStrategy: FlowStrategy>));
impl_generic_enum_variant!(
    FlowEvent::TriggerAdded(FlowEventTriggerAdded<TFlowStrategy: FlowStrategy>)
);
impl_generic_enum_variant!(
    FlowEvent::TaskScheduled(FlowEventTaskScheduled<TFlowStrategy: FlowStrategy>)
);
impl_generic_enum_variant!(
    FlowEvent::TaskFinished(FlowEventTaskFinished<TFlowStrategy: FlowStrategy>)
);
impl_generic_enum_variant!(FlowEvent::Cancelled(FlowEventCancelled<TFlowStrategy: FlowStrategy>));

/////////////////////////////////////////////////////////////////////////////////////////
