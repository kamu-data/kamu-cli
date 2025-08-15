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
    pub for_breaking_change: FlowTriggerBreakingChangeRule,
}

impl From<ReactiveRule> for FlowTriggerReactiveRule {
    fn from(value: ReactiveRule) -> Self {
        Self {
            for_new_data: value.for_new_data.into(),
            for_breaking_change: value.for_breaking_change.into(),
        }
    }
}

#[derive(Union, PartialEq, Eq)]
pub enum FlowTriggerBatchingRule {
    Immediate(FlowTriggerBatchingRuleImmediate),
    Buffering(FlowTriggerBatchingRuleBuffering),
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTriggerBatchingRuleImmediate {
    pub dummy: bool,
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTriggerBatchingRuleBuffering {
    pub min_records_to_await: u64,
    pub max_batching_interval: TimeDelta,
}

impl From<BatchingRule> for FlowTriggerBatchingRule {
    fn from(value: BatchingRule) -> Self {
        match value {
            BatchingRule::Immediate => {
                Self::Immediate(FlowTriggerBatchingRuleImmediate { dummy: true })
            }
            BatchingRule::Buffering {
                min_records_to_await,
                max_batching_interval,
            } => Self::Buffering(FlowTriggerBatchingRuleBuffering {
                min_records_to_await,
                max_batching_interval: max_batching_interval.into(),
            }),
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
    Schedule(FlowTriggerScheduleInput),
    Reactive(FlowTriggerReactiveInput),
}

#[derive(OneofObject, Debug)]
pub enum FlowTriggerScheduleInput {
    TimeDelta(TimeDeltaInput),
    /// Supported CRON syntax: min hour dayOfMonth month dayOfWeek
    Cron5ComponentExpression(String),
}

#[derive(InputObject, Debug)]
pub struct FlowTriggerReactiveInput {
    pub for_new_data: FlowTriggerBatchingRuleInput,
    pub for_breaking_change: FlowTriggerBreakingChangeRule,
}

#[derive(OneofObject, Debug)]
pub enum FlowTriggerBatchingRuleInput {
    Immediate(FlowTriggerBatchingRuleImmediateInput),
    Buffering(FlowTriggerBatchingRuleBufferingInput),
}

#[derive(InputObject, Debug)]
pub struct FlowTriggerBatchingRuleImmediateInput {
    pub dummy: bool,
}

#[derive(InputObject, Debug)]
pub struct FlowTriggerBatchingRuleBufferingInput {
    pub min_records_to_await: u64,
    pub max_batching_interval: TimeDeltaInput,
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
                    FlowTriggerScheduleInput::TimeDelta(td) => {
                        Schedule::TimeDelta(ScheduleTimeDelta { every: td.into() })
                    }
                    FlowTriggerScheduleInput::Cron5ComponentExpression(
                        cron_5component_expression,
                    ) => Schedule::try_from_5component_cron_expression(&cron_5component_expression)
                        .map_err(|err| Self::Error {
                            reason: err.to_string(),
                        })?,
                };
                Ok(FlowTriggerRule::Schedule(schedule_rule))
            }
            FlowTriggerInput::Reactive(reactive_input) => {
                let batching_rule = match reactive_input.for_new_data {
                    FlowTriggerBatchingRuleInput::Immediate(_) => BatchingRule::immediate(),
                    FlowTriggerBatchingRuleInput::Buffering(collecting_input) => {
                        BatchingRule::try_buffering(
                            collecting_input.min_records_to_await,
                            collecting_input.max_batching_interval.into(),
                        )
                        .map_err(|e| Self::Error {
                            reason: e.to_string(),
                        })?
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
