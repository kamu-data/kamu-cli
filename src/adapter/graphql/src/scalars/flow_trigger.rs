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

/// Rule that defines when a flow runs automatically
#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTrigger {
    /// Whether the trigger is inactive: paused by a user or auto-stopped
    pub paused: bool,
    /// Schedule rule, null if the trigger is reactive
    pub schedule: Option<FlowTriggerScheduleRule>,
    /// Reactive rule, null if the trigger runs on a schedule
    pub reactive: Option<FlowTriggerReactiveRule>,
    /// When the trigger stops itself after failures
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

/// Schedule of a trigger: a fixed interval or a cron expression
#[derive(Union, PartialEq, Eq)]
pub enum FlowTriggerScheduleRule {
    TimeDelta(TimeDelta),
    Cron(Cron5ComponentExpression),
}

/// Rule that runs a flow in reaction to changes of its inputs
#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowTriggerReactiveRule {
    /// How new input data is batched before the flow runs
    pub for_new_data: FlowTriggerBatchingRule,
    /// How to react when an input's history is rewritten
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

/// How new input data is accumulated before a reactive flow runs
#[derive(Union, PartialEq, Eq)]
pub enum FlowTriggerBatchingRule {
    Immediate(FlowTriggerBatchingRuleImmediate),
    Buffering(FlowTriggerBatchingRuleBuffering),
}

/// Run the flow as soon as new input data appears
#[derive(SimpleObject, InputObject, Debug, PartialEq, Eq)]
#[graphql(input_name = "FlowTriggerBatchingRuleImmediateInput")]
pub struct FlowTriggerBatchingRuleImmediate {
    /// Placeholder without meaning: always true in responses, ignored in
    /// requests
    pub dummy: bool,
}

/// Accumulate new input data until enough records arrive or the interval
/// passes
#[derive(SimpleObject, InputObject, Debug, PartialEq, Eq)]
#[graphql(input_name = "FlowTriggerBatchingRuleBufferingInput")]
pub struct FlowTriggerBatchingRuleBuffering {
    /// Number of new input records that runs the flow
    pub min_records_to_await: u64,
    /// Longest wait after the first new data before the flow runs anyway
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

/// When a trigger stops itself after failed runs
#[derive(Union, PartialEq, Eq)]
pub enum FlowTriggerStopPolicy {
    Never(FlowTriggerStopPolicyNever),
    AfterConsecutiveFailures(FlowTriggerStopPolicyAfterConsecutiveFailures),
}

/// Trigger never stops because of failures
#[derive(SimpleObject, InputObject, Debug, PartialEq, Eq)]
#[graphql(input_name = "FlowTriggerStopPolicyNeverInput")]
pub struct FlowTriggerStopPolicyNever {
    /// Placeholder without meaning: always true in responses, ignored in
    /// requests
    pub dummy: bool,
}

/// Trigger stops after a number of failed runs in a row
#[derive(SimpleObject, InputObject, Debug, PartialEq, Eq)]
#[graphql(input_name = "FlowTriggerStopPolicyAfterConsecutiveFailuresInput")]
pub struct FlowTriggerStopPolicyAfterConsecutiveFailures {
    /// Number of failed runs in a row that stops the trigger, from 1 to 10
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

/// When a trigger stops itself after failed runs
#[derive(OneofObject, Debug)]
pub enum FlowTriggerStopPolicyInput {
    /// Never stop the trigger because of failures
    Never(FlowTriggerStopPolicyNever),
    /// Stop the trigger after a number of failed runs in a row
    AfterConsecutiveFailures(FlowTriggerStopPolicyAfterConsecutiveFailures),
}

// For tests only!
impl Default for FlowTriggerStopPolicyInput {
    fn default() -> Self {
        Self::AfterConsecutiveFailures(FlowTriggerStopPolicyAfterConsecutiveFailures {
            max_failures: 1,
        })
    }
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

/// Rule that defines when a flow runs automatically
#[derive(OneofObject, Debug)]
pub enum FlowTriggerRuleInput {
    /// Run on a schedule
    Schedule(FlowTriggerRuleScheduleInput),
    /// Run in reaction to changes of inputs
    Reactive(FlowTriggerRuleReactiveInput),
}

/// Schedule of a trigger: a fixed interval or a cron expression
#[derive(OneofObject, Debug)]
pub enum FlowTriggerRuleScheduleInput {
    /// Run at a fixed interval
    TimeDelta(TimeDelta),
    /// Supported CRON syntax: min hour dayOfMonth month dayOfWeek
    Cron5ComponentExpression(String),
}

/// Rule that runs a flow in reaction to changes of its inputs
#[derive(InputObject, Debug)]
pub struct FlowTriggerRuleReactiveInput {
    /// How new input data is batched before the flow runs
    pub for_new_data: FlowTriggerBatchingRuleInput,
    /// How to react when an input's history is rewritten
    pub for_breaking_change: FlowTriggerBreakingChangeRule,
}

/// How new input data is accumulated before a reactive flow runs
#[derive(OneofObject, Debug)]
pub enum FlowTriggerBatchingRuleInput {
    /// Run as soon as new input data appears
    Immediate(FlowTriggerBatchingRuleImmediate),
    /// Accumulate new input data first
    Buffering(FlowTriggerBatchingRuleBuffering),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Cron schedule of a trigger
#[derive(SimpleObject, PartialEq, Eq)]
pub struct Cron5ComponentExpression {
    /// Cron expression: min hour dayOfMonth month dayOfWeek
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
            Self::Schedule(_) => match flow_type {
                DatasetFlowType::Ingest | DatasetFlowType::HardCompaction => return Ok(()),
                DatasetFlowType::ExecuteTransform
                | DatasetFlowType::Reset
                | DatasetFlowType::ResetToMetadata => {}
            },
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
