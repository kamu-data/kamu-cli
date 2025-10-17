// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;

use crate::mutations::{
    FlowInvalidTriggerInputError,
    FlowInvalidTriggerStopPolicyInputError,
    FlowTypeIsNotSupported,
};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTrigger {
    pub paused: bool,
    pub schedule: Option<FlowTriggerScheduleRule>,
    pub reactive: Option<FlowTriggerReactiveRule>,
    pub stop_policy: FlowTriggerStopPolicy,
}

impl From<fs::Schedule> for FlowTriggerScheduleRule {
    fn from(value: fs::Schedule) -> Self {
        match value {
            fs::Schedule::TimeDelta(time_delta) => Self::TimeDelta(time_delta.every.into()),
            fs::Schedule::Cron(cron) => Self::Cron(cron.into()),
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

impl From<fs::ReactiveRule> for FlowTriggerReactiveRule {
    fn from(value: fs::ReactiveRule) -> Self {
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

impl From<fs::BatchingRule> for FlowTriggerBatchingRule {
    fn from(value: fs::BatchingRule) -> Self {
        match value {
            fs::BatchingRule::Immediate => {
                Self::Immediate(FlowTriggerBatchingRuleImmediate { dummy: true })
            }
            fs::BatchingRule::Buffering {
                min_records_to_await,
                max_batching_interval,
            } => Self::Buffering(FlowTriggerBatchingRuleBuffering {
                min_records_to_await,
                max_batching_interval: max_batching_interval.into(),
            }),
        }
    }
}

#[derive(Union, PartialEq, Eq)]
pub enum FlowTriggerStopPolicy {
    Never(FlowTriggerStopPolicyNever),
    AfterConsecutiveFailures(FlowTriggerStopPolicyAfterConsecutiveFailures),
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTriggerStopPolicyNever {
    pub dummy: bool,
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTriggerStopPolicyAfterConsecutiveFailures {
    pub max_failures: u32,
}

impl From<fs::FlowTriggerStopPolicy> for FlowTriggerStopPolicy {
    fn from(value: fs::FlowTriggerStopPolicy) -> Self {
        match value {
            fs::FlowTriggerStopPolicy::Never => {
                Self::Never(FlowTriggerStopPolicyNever { dummy: true })
            }
            fs::FlowTriggerStopPolicy::AfterConsecutiveFailures { failures_count } => {
                Self::AfterConsecutiveFailures(FlowTriggerStopPolicyAfterConsecutiveFailures {
                    max_failures: failures_count.into(),
                })
            }
        }
    }
}

impl From<kamu_flow_system::FlowTriggerState> for FlowTrigger {
    fn from(value: kamu_flow_system::FlowTriggerState) -> Self {
        Self {
            paused: !value.is_active(),
            reactive: if let fs::FlowTriggerRule::Reactive(condition) = value.rule {
                Some(condition.into())
            } else {
                None
            },
            schedule: if let fs::FlowTriggerRule::Schedule(schedule_rule) = value.rule {
                Some(schedule_rule.into())
            } else {
                None
            },
            stop_policy: value.stop_policy.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject, Debug)]
pub enum FlowTriggerStopPolicyInput {
    Never(FlowTriggerStopPolicyNeverInput),
    AfterConsecutiveFailures(FlowTriggerStopPolicyAfterConsecutiveFailuresInput),
}

// For tests only!
impl Default for FlowTriggerStopPolicyInput {
    fn default() -> Self {
        Self::AfterConsecutiveFailures(FlowTriggerStopPolicyAfterConsecutiveFailuresInput {
            max_failures: 1,
        })
    }
}

#[derive(InputObject, Debug)]
pub struct FlowTriggerStopPolicyNeverInput {
    pub dummy: bool,
}

#[derive(InputObject, Debug)]
pub struct FlowTriggerStopPolicyAfterConsecutiveFailuresInput {
    pub max_failures: u32,
}

impl TryFrom<FlowTriggerStopPolicyInput> for fs::FlowTriggerStopPolicy {
    type Error = FlowInvalidTriggerStopPolicyInputError;

    fn try_from(value: FlowTriggerStopPolicyInput) -> std::result::Result<Self, Self::Error> {
        match value {
            FlowTriggerStopPolicyInput::Never(_) => Ok(fs::FlowTriggerStopPolicy::Never),
            FlowTriggerStopPolicyInput::AfterConsecutiveFailures(after_failures_input) => {
                Ok(fs::FlowTriggerStopPolicy::AfterConsecutiveFailures {
                    failures_count: fs::ConsecutiveFailuresCount::try_new(
                        after_failures_input.max_failures,
                    )
                    .map_err(|e| Self::Error {
                        reason: e.to_string(),
                    })?,
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(OneofObject, Debug)]
pub enum FlowTriggerRuleInput {
    Schedule(FlowTriggerRuleScheduleInput),
    Reactive(FlowTriggerRuleReactiveInput),
}

#[derive(OneofObject, Debug)]
pub enum FlowTriggerRuleScheduleInput {
    TimeDelta(TimeDeltaInput),
    /// Supported CRON syntax: min hour dayOfMonth month dayOfWeek
    Cron5ComponentExpression(String),
}

#[derive(InputObject, Debug)]
pub struct FlowTriggerRuleReactiveInput {
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

impl From<fs::ScheduleCron> for Cron5ComponentExpression {
    fn from(value: fs::ScheduleCron) -> Self {
        Self {
            cron_5component_expression: value.source_5component_cron_expression,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowTriggerRuleInput {
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

impl TryFrom<FlowTriggerRuleInput> for fs::FlowTriggerRule {
    type Error = FlowInvalidTriggerInputError;

    fn try_from(value: FlowTriggerRuleInput) -> std::result::Result<Self, Self::Error> {
        match value {
            FlowTriggerRuleInput::Schedule(schedule_input) => {
                let schedule_rule = match schedule_input {
                    FlowTriggerRuleScheduleInput::TimeDelta(td) => {
                        fs::Schedule::TimeDelta(fs::ScheduleTimeDelta { every: td.into() })
                    }
                    FlowTriggerRuleScheduleInput::Cron5ComponentExpression(
                        cron_5component_expression,
                    ) => fs::Schedule::try_from_5component_cron_expression(
                        &cron_5component_expression,
                    )
                    .map_err(|err| Self::Error {
                        reason: err.to_string(),
                    })?,
                };
                Ok(fs::FlowTriggerRule::Schedule(schedule_rule))
            }
            FlowTriggerRuleInput::Reactive(reactive_input) => {
                let batching_rule = match reactive_input.for_new_data {
                    FlowTriggerBatchingRuleInput::Immediate(_) => fs::BatchingRule::immediate(),
                    FlowTriggerBatchingRuleInput::Buffering(collecting_input) => {
                        fs::BatchingRule::try_buffering(
                            collecting_input.min_records_to_await,
                            collecting_input.max_batching_interval.into(),
                        )
                        .map_err(|e| Self::Error {
                            reason: e.to_string(),
                        })?
                    }
                };

                Ok(fs::FlowTriggerRule::Reactive(fs::ReactiveRule::new(
                    batching_rule,
                    reactive_input.for_breaking_change.into(),
                )))
            }
        }
    }
}
