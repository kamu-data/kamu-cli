// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use cron::Schedule as CronSchedule;
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

/// Represents dataset update settings
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    /// Time-delta based schedule
    TimeDelta(ScheduleTimeDelta),
    /// Cron-based schedule
    CronExpression(CronSchedule),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleTimeDelta {
    pub every: chrono::Duration,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ScheduleError {
    #[error(transparent)]
    InvalidCronExptression(#[from] InvalidCronExpressionError),

    #[error(transparent)]
    CronExpressionIterationExceed(#[from] CronExpressionIterationError),
}

#[derive(Error, Debug)]
#[error("Cron expression {expression} is invalid")]
pub struct InvalidCronExpressionError {
    pub expression: String,
}

#[derive(Error, Debug)]
#[error("Cron expression {expression} iteration has been exceed")]
pub struct CronExpressionIterationError {
    pub expression: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Schedule {
    pub fn validate_cron_expression(
        cron_expression: String,
    ) -> Result<CronSchedule, ScheduleError> {
        let schedule = match CronSchedule::from_str(&cron_expression) {
            Err(_) => {
                return Err(ScheduleError::InvalidCronExptression(
                    InvalidCronExpressionError {
                        expression: cron_expression.clone(),
                    },
                ));
            }
            Ok(cron_schedule) => cron_schedule,
        };
        match schedule.upcoming(Utc).next() {
            Some(_) => Ok(schedule),
            None => Err(ScheduleError::CronExpressionIterationExceed(
                CronExpressionIterationError {
                    expression: cron_expression.clone(),
                },
            )),
        }
    }
    pub fn next_activation_time(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, ScheduleError> {
        match self {
            Schedule::TimeDelta(td) => Ok(now + td.every),
            Schedule::CronExpression(ce) => match ce.upcoming(Utc).next() {
                Some(nct) => Ok(nct),
                None => Err(ScheduleError::CronExpressionIterationExceed(
                    CronExpressionIterationError {
                        expression: ce.to_string(),
                    },
                )),
            },
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<chrono::Duration> for Schedule {
    fn from(value: chrono::Duration) -> Self {
        Self::TimeDelta(ScheduleTimeDelta { every: value })
    }
}

impl From<String> for Schedule {
    fn from(value: String) -> Self {
        Self::CronExpression(CronSchedule::from_str(&value).unwrap())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
