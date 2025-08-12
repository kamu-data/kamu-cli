// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{
    BatchingRule,
    FlowTriggerRule,
    ReactiveRule,
    Schedule,
    ScheduleCron,
    ScheduleTimeDelta,
};

use crate::mutations::{FlowInvalidTriggerInputError, FlowTypeIsNotSupported};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTrigger {
    pub paused: bool,
    pub schedule: Option<FlowTriggerScheduleRule>,
    pub reactive: Option<FlowTriggerReactiveRule>,
}

impl From<Schedule> for FlowTriggerScheduleRule {
    fn from(value: Schedule) -> Self {
        match value {
            Schedule::TimeDelta(time_delta) => Self::TimeDelta(time_delta.every.into()),
            Schedule::Cron(cron) => Self::Cron(cron.into()),
        }
    }
}

#[derive(Union, PartialEq, Eq)]
pub enum FlowTriggerScheduleRule {
    TimeDelta(TimeDelta),
    Cron(Cron5ComponentExpression),
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTriggerReactiveRule {
    pub for_new_data: FlowTriggerBatchingRule,
    pub for_breaking_change: BreakingChangeRule,
}

impl From<ReactiveRule> for FlowTriggerReactiveRule {
    fn from(value: ReactiveRule) -> Self {
        Self {
            for_new_data: value.for_new_data.into(),
            for_breaking_change: value.for_breaking_change.into(),
        }
    }
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTriggerBatchingRule {
    pub min_records_to_await: u64,
    pub max_batching_interval: Option<TimeDelta>,
}

impl From<BatchingRule> for FlowTriggerBatchingRule {
    fn from(value: BatchingRule) -> Self {
        Self {
            min_records_to_await: value.min_records_to_await(),
            max_batching_interval: value.max_batching_interval().map(Into::into),
        }
    }
}

impl From<kamu_flow_system::FlowTriggerState> for FlowTrigger {
    fn from(value: kamu_flow_system::FlowTriggerState) -> Self {
        Self {
            paused: !value.is_active(),
            reactive: if let FlowTriggerRule::Reactive(condition) = value.rule {
                Some(condition.into())
            } else {
                None
            },
            schedule: if let FlowTriggerRule::Schedule(schedule_rule) = value.rule {
                Some(schedule_rule.into())
            } else {
                None
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject, Debug)]
pub enum FlowTriggerInput {
    Schedule(ScheduleInput),
    Reactive(ReactiveInput),
}

#[derive(OneofObject, Debug)]
pub enum ScheduleInput {
    TimeDelta(TimeDeltaInput),
    /// Supported CRON syntax: min hour dayOfMonth month dayOfWeek
    Cron5ComponentExpression(String),
}

#[derive(InputObject, Debug)]
pub struct ReactiveInput {
    pub for_new_data: BatchingInput,
    pub for_breaking_change: BreakingChangeRule,
}

#[derive(InputObject, Debug)]
pub struct BatchingInput {
    pub min_records_to_await: u64,
    pub max_batching_interval: Option<TimeDeltaInput>,
}

#[derive(InputObject, Debug)]
pub struct TimeDeltaInput {
    pub every: u32,
    pub unit: TimeUnit,
}

impl From<TimeDeltaInput> for chrono::Duration {
    fn from(value: TimeDeltaInput) -> Self {
        let every = i64::from(value.every);
        match value.unit {
            TimeUnit::Weeks => chrono::Duration::try_weeks(every).unwrap(),
            TimeUnit::Days => chrono::Duration::try_days(every).unwrap(),
            TimeUnit::Hours => chrono::Duration::try_hours(every).unwrap(),
            TimeUnit::Minutes => chrono::Duration::try_minutes(every).unwrap(),
        }
    }
}

impl From<&TimeDeltaInput> for chrono::Duration {
    fn from(value: &TimeDeltaInput) -> Self {
        let every = i64::from(value.every);
        match value.unit {
            TimeUnit::Weeks => chrono::Duration::try_weeks(every).unwrap(),
            TimeUnit::Days => chrono::Duration::try_days(every).unwrap(),
            TimeUnit::Hours => chrono::Duration::try_hours(every).unwrap(),
            TimeUnit::Minutes => chrono::Duration::try_minutes(every).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct Cron5ComponentExpression {
    pub cron_5component_expression: String,
}

impl From<ScheduleCron> for Cron5ComponentExpression {
    fn from(value: ScheduleCron) -> Self {
        Self {
            cron_5component_expression: value.source_5component_cron_expression,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowTriggerInput {
    pub fn check_type_compatible(
        &self,
        flow_type: DatasetFlowType,
    ) -> Result<(), FlowTypeIsNotSupported> {
        match self {
            Self::Schedule(_) => {
                if flow_type == DatasetFlowType::Ingest {
                    return Ok(());
                }
            }
            Self::Reactive(_) => {
                if flow_type == DatasetFlowType::ExecuteTransform {
                    return Ok(());
                }
            }
        }
        Err(FlowTypeIsNotSupported)
    }
}

impl TryFrom<FlowTriggerInput> for FlowTriggerRule {
    type Error = FlowInvalidTriggerInputError;

    fn try_from(value: FlowTriggerInput) -> std::result::Result<Self, Self::Error> {
        match value {
            FlowTriggerInput::Schedule(schedule_input) => {
                let schedule_rule = match schedule_input {
                    ScheduleInput::TimeDelta(td) => {
                        Schedule::TimeDelta(ScheduleTimeDelta { every: td.into() })
                    }
                    ScheduleInput::Cron5ComponentExpression(cron_5component_expression) => {
                        Schedule::try_from_5component_cron_expression(&cron_5component_expression)
                            .map_err(|err| Self::Error {
                            reason: err.to_string(),
                        })?
                    }
                };
                Ok(FlowTriggerRule::Schedule(schedule_rule))
            }
            FlowTriggerInput::Reactive(reactive_input) => {
                let batching_rule = match BatchingRule::try_new(
                    reactive_input.for_new_data.min_records_to_await,
                    reactive_input
                        .for_new_data
                        .max_batching_interval
                        .map(Into::into),
                ) {
                    Ok(rule) => rule,
                    Err(e) => {
                        return Err(Self::Error {
                            reason: e.to_string(),
                        });
                    }
                };
                Ok(FlowTriggerRule::Reactive(ReactiveRule::new(
                    batching_rule,
                    reactive_input.for_breaking_change.into(),
                )))
            }
        }
    }
}
