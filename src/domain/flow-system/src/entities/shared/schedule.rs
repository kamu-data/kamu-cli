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
    CronExpression(ScheduleCronExpression),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleTimeDelta {
    pub every: chrono::Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleCronExpression {
    pub source: CronExpressionSource,
    pub schedule: CronSchedule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronExpressionSource {
    pub min: String,
    pub hour: String,
    pub day_of_month: String,
    pub month: String,
    pub day_of_week: String,
}

impl CronExpressionSource {
    pub fn as_cron_expression_string_with_sec(&self) -> String {
        format!(
            "0 {} {} {} {} {}",
            self.min, self.hour, self.day_of_month, self.month, self.day_of_week
        )
    }
}

impl Default for CronExpressionSource {
    fn default() -> Self {
        // Means run every 1 minute
        Self {
            min: "*".to_string(),
            hour: "*".to_string(),
            day_of_month: "*".to_string(),
            month: "*".to_string(),
            day_of_week: "*".to_string(),
        }
    }
}

impl std::fmt::Display for CronExpressionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} {} {} {} {}",
            self.min, self.hour, self.day_of_month, self.month, self.day_of_week
        )
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ScheduleError {
    #[error(transparent)]
    InvalidCronExpression(#[from] InvalidCronExpressionError),

    #[error(transparent)]
    CronExpressionIterationExceed(#[from] CronExpressionIterationError),
}

#[derive(Error, Debug)]
#[error("Cron expression {expression} is invalid")]
pub struct InvalidCronExpressionError {
    pub expression: String,
}

#[derive(Error, Debug)]
#[error("Cron expression {expression} iteration has been exceeded")]
pub struct CronExpressionIterationError {
    pub expression: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Schedule {
    pub fn new_cron_schedule(source: CronExpressionSource) -> Result<Schedule, ScheduleError> {
        let cron_as_string_with_dummy_seconds = source.as_cron_expression_string_with_sec();

        let schedule = match CronSchedule::from_str(&cron_as_string_with_dummy_seconds) {
            Err(_) => {
                return Err(ScheduleError::InvalidCronExpression(
                    InvalidCronExpressionError {
                        expression: source.to_string(),
                    },
                ));
            }
            Ok(cron_schedule) => cron_schedule,
        };

        match schedule.upcoming(Utc).next() {
            Some(_) => Ok(Schedule::CronExpression(ScheduleCronExpression {
                source,
                schedule,
            })),
            None => Err(ScheduleError::CronExpressionIterationExceed(
                CronExpressionIterationError {
                    expression: source.to_string(),
                },
            )),
        }
    }

    pub fn next_activation_time(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Schedule::TimeDelta(td) => Some(now + td.every),
            Schedule::CronExpression(ce) => ce.schedule.upcoming(Utc).next(),
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

#[cfg(test)]
mod tests {
    use chrono::prelude::*;

    use super::*;

    #[test]
    fn test_validate_invalid_expression() {
        // Try to pass invalid cron expression
        let source_with_invalid_parts = CronExpressionSource {
            min: "invalid".to_string(),
            ..Default::default()
        };
        let err_result =
            Schedule::new_cron_schedule(source_with_invalid_parts.clone()).unwrap_err();

        assert_eq!(
            err_result.to_string(),
            format!("Cron expression {} is invalid", source_with_invalid_parts),
        );
    }

    #[test]
    fn test_get_next_time_from_expression() {
        let cron_source = CronExpressionSource {
            min: "0".to_string(),
            hour: "0".to_string(),
            day_of_month: "1".to_string(),
            month: "JAN".to_string(),
            day_of_week: "?".to_string(),
        };
        let cron_schedule = Schedule::new_cron_schedule(cron_source).unwrap();

        let current_year = Utc::now().year();
        let expected_time = Utc
            .with_ymd_and_hms(current_year + 1, 1, 1, 0, 0, 0)
            .unwrap();

        let cron_time = cron_schedule.next_activation_time(Utc::now()).unwrap();
        assert_eq!(cron_time, expected_time);
    }
}
