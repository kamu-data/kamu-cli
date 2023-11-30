// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

use chrono::{DateTime, Utc};

/// Represents dataset update settings
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// Time-delta based schedule
    TimeDelta(ScheduleTimeDelta),
    /// Cron-based schedule
    CronExpression(ScheduleCronExpression),
    /// Reactive schedule
    Reactive(ScheduleReactive),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleTimeDelta {
    pub every: chrono::Duration,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleCronExpression {
    pub expression: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleReactive {
    pub throttling_period: Option<chrono::Duration>,
    pub minimal_data_batch: Option<i32>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Schedule {
    pub fn is_proactive(&self) -> bool {
        match self {
            Schedule::TimeDelta(_) => true,
            Schedule::CronExpression(_) => true,
            Schedule::Reactive(_) => false,
        }
    }

    pub fn next_activation_time(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Schedule::TimeDelta(td) => Some(now + td.every),
            Schedule::CronExpression(_) => {
                unimplemented!()
            }
            _ => None,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
