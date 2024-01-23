// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{FlowConfigurationRule, Schedule};

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfiguration {
    pub paused: bool,
    pub schedule: Option<FlowConfigurationSchedule>,
    pub batching: Option<FlowConfigurationBatching>,
}

impl From<kamu_flow_system::FlowConfigurationState> for FlowConfiguration {
    fn from(value: kamu_flow_system::FlowConfigurationState) -> Self {
        Self {
            paused: !value.is_active(),
            batching: if let FlowConfigurationRule::StartCondition(condition) = &value.rule {
                Some(FlowConfigurationBatching {
                    throttling_period: condition.throttling_period.map(|tp| tp.into()),
                    minimal_data_batch: condition.minimal_data_batch,
                })
            } else {
                None
            },
            schedule: if let FlowConfigurationRule::Schedule(schedule) = value.rule {
                match schedule {
                    Schedule::TimeDelta(time_delta) => Some(FlowConfigurationSchedule::TimeDelta(
                        time_delta.every.into(),
                    )),
                    Schedule::CronExpression(cron) => {
                        Some(FlowConfigurationSchedule::Cron(cron.into()))
                    }
                }
            } else {
                None
            },
        }
    }
}

#[derive(Union, Clone, PartialEq, Eq)]
pub enum FlowConfigurationSchedule {
    TimeDelta(TimeDelta),
    Cron(CronExpression),
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowConfigurationBatching {
    pub throttling_period: Option<TimeDelta>,
    pub minimal_data_batch: Option<i32>,
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct CronExpression {
    pub min: String,
    pub hour: String,
    pub day_of_month: String,
    pub month: String,
    pub day_of_week: String,
}

impl From<kamu_flow_system::ScheduleCronExpression> for CronExpression {
    fn from(value: kamu_flow_system::ScheduleCronExpression) -> Self {
        Self {
            min: value.source.min,
            hour: value.source.hour,
            day_of_month: value.source.day_of_month,
            month: value.source.month,
            day_of_week: value.source.day_of_week,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct TimeDelta {
    pub every: u32,
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
        let num_weeks = value.num_weeks();
        if (value - chrono::Duration::weeks(num_weeks)).is_zero() {
            return Self {
                every: num_weeks as u32,
                unit: TimeUnit::Weeks,
            };
        }

        let num_days = value.num_days();
        if (value - chrono::Duration::days(num_days)).is_zero() {
            return Self {
                every: num_days as u32,
                unit: TimeUnit::Days,
            };
        }

        let num_hours = value.num_hours();
        if (value - chrono::Duration::hours(num_hours)).is_zero() {
            return Self {
                every: num_hours as u32,
                unit: TimeUnit::Hours,
            };
        }

        let num_minutes = value.num_minutes();
        if (value - chrono::Duration::minutes(num_minutes)).is_zero() {
            return Self {
                every: num_minutes as u32,
                unit: TimeUnit::Minutes,
            };
        }

        unreachable!(
            "Expecting intervals not smaller than 1 minute that are clearly dividable by unit"
        );
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
