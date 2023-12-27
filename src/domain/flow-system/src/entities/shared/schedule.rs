// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

/////////////////////////////////////////////////////////////////////////////////////////

/// Represents dataset update settings
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// Time-delta based schedule
    TimeDelta(ScheduleTimeDelta),
    /// Cron-based schedule
    CronExpression(ScheduleCronExpression),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleTimeDelta {
    pub every: chrono::Duration,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleCronExpression {
    pub cron_expression: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Schedule {
    pub fn next_activation_time(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Schedule::TimeDelta(td) => now + td.every,
            Schedule::CronExpression(_) => {
                unimplemented!()
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<chrono::Duration> for Schedule {
    fn from(value: chrono::Duration) -> Self {
        Self::TimeDelta(ScheduleTimeDelta { every: value })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
