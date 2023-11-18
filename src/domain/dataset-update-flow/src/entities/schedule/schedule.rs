// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

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
