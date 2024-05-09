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
    CompactionRule,
    FlowConfigurationRule,
    Schedule,
    ScheduleCron,
};

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfiguration {
    pub paused: bool,
    pub schedule: Option<FlowConfigurationSchedule>,
    pub batching: Option<FlowConfigurationBatching>,
    pub compaction: Option<FlowConfigurationCompaction>,
}

impl From<kamu_flow_system::FlowConfigurationState> for FlowConfiguration {
    fn from(value: kamu_flow_system::FlowConfigurationState) -> Self {
        Self {
            paused: !value.is_active(),
            batching: if let FlowConfigurationRule::BatchingRule(condition) = &value.rule {
                Some((*condition).into())
            } else {
                None
            },
            schedule: if let FlowConfigurationRule::Schedule(schedule) = &value.rule {
                match schedule {
                    Schedule::TimeDelta(time_delta) => Some(FlowConfigurationSchedule::TimeDelta(
                        time_delta.every.into(),
                    )),
                    Schedule::Cron(cron) => {
                        Some(FlowConfigurationSchedule::Cron(cron.clone().into()))
                    }
                }
            } else {
                None
            },
            compaction: if let FlowConfigurationRule::CompactionRule(args) = value.rule {
                Some(args.into())
            } else {
                None
            },
        }
    }
}

#[derive(Union, Clone, PartialEq, Eq)]
pub enum FlowConfigurationSchedule {
    TimeDelta(TimeDelta),
    Cron(Cron5ComponentExpression),
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfigurationBatching {
    pub min_records_to_await: u64,
    pub max_batching_interval: TimeDelta,
}

impl From<BatchingRule> for FlowConfigurationBatching {
    fn from(value: BatchingRule) -> Self {
        Self {
            min_records_to_await: value.min_records_to_await(),
            max_batching_interval: (*value.max_batching_interval()).into(),
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfigurationCompaction {
    pub max_slice_size: u64,
    pub max_slice_records: u64,
}

impl From<CompactionRule> for FlowConfigurationCompaction {
    fn from(value: CompactionRule) -> Self {
        Self {
            max_slice_size: value.max_slice_size(),
            max_slice_records: value.max_slice_records(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
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

/////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
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
            value.num_seconds() > 0,
            "Positive interval expected, but received [{value}]"
        );

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

        panic!(
            "Expecting intervals not smaller than 1 minute that are clearly dividable by unit, \
             but received [{value}]"
        );
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
