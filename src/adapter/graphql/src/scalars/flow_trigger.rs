// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{BatchingRule, FlowTriggerRule, Schedule, ScheduleCron, ScheduleTimeDelta};

use crate::mutations::{FlowInvalidTriggerInputError, FlowTypeIsNotSupported};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTrigger {
    pub paused: bool,
    pub schedule: Option<FlowTriggerScheduleRule>,
    pub batching: Option<FlowTriggerBatchingRule>,
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
pub struct FlowTriggerBatchingRule {
    pub min_records_to_await: u64,
    pub max_batching_interval: TimeDelta,
}

impl From<BatchingRule> for FlowTriggerBatchingRule {
    fn from(value: BatchingRule) -> Self {
        Self {
            min_records_to_await: value.min_records_to_await(),
            max_batching_interval: (*value.max_batching_interval()).into(),
        }
    }
}

impl From<kamu_flow_system::FlowTriggerState> for FlowTrigger {
    fn from(value: kamu_flow_system::FlowTriggerState) -> Self {
        Self {
            paused: !value.is_active(),
            batching: if let FlowTriggerRule::Batching(condition) = value.rule {
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

#[derive(OneofObject)]
pub enum FlowTriggerInput {
    Schedule(ScheduleInput),
    Batching(BatchingInput),
}

#[derive(OneofObject)]
pub enum ScheduleInput {
    TimeDelta(TimeDeltaInput),
    /// Supported CRON syntax: min hour dayOfMonth month dayOfWeek
    Cron5ComponentExpression(String),
}

#[derive(InputObject)]
pub struct BatchingInput {
    pub min_records_to_await: u64,
    pub max_batching_interval: TimeDeltaInput,
}

#[derive(InputObject)]
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

#[derive(SimpleObject, PartialEq, Eq)]
pub struct TimeDelta {
    pub every: i64,
    pub unit: TimeUnit,
}

#[derive(Enum, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Minutes,
    Hours,
    Days,
    Weeks,
}

impl From<chrono::Duration> for TimeDelta {
    fn from(value: chrono::Duration) -> Self {
        assert!(
            value.num_seconds() >= 0,
            "Positive or zero interval expected, but received [{value}]"
        );

        if value.is_zero() {
            return Self {
                every: 0,
                unit: TimeUnit::Minutes,
            };
        }

        let num_weeks = value.num_weeks();
        if (value - chrono::Duration::try_weeks(num_weeks).unwrap()).is_zero() {
            return Self {
                every: num_weeks,
                unit: TimeUnit::Weeks,
            };
        }

        let num_days = value.num_days();
        if (value - chrono::Duration::try_days(num_days).unwrap()).is_zero() {
            return Self {
                every: num_days,
                unit: TimeUnit::Days,
            };
        }

        let num_hours = value.num_hours();
        if (value - chrono::Duration::try_hours(num_hours).unwrap()).is_zero() {
            return Self {
                every: num_hours,
                unit: TimeUnit::Hours,
            };
        }

        let num_minutes = value.num_minutes();
        if (value - chrono::Duration::try_minutes(num_minutes).unwrap()).is_zero() {
            return Self {
                every: num_minutes,
                unit: TimeUnit::Minutes,
            };
        }

        panic!("Expecting intervals that are clearly dividable by unit, but received [{value}]");
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
            Self::Batching(_) => {
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
            FlowTriggerInput::Batching(batching_input) => {
                let batching_rule = match BatchingRule::new_checked(
                    batching_input.min_records_to_await,
                    batching_input.max_batching_interval.into(),
                ) {
                    Ok(rule) => rule,
                    Err(e) => {
                        return Err(Self::Error {
                            reason: e.to_string(),
                        });
                    }
                };
                Ok(FlowTriggerRule::Batching(batching_rule))
            }
        }
    }
}
