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
    pub fn validate_cron_expression(
        cron_expression: String,
    ) -> Result<CronSchedule, ScheduleError> {
        let schedule = match CronSchedule::from_str(&cron_expression) {
            Err(_) => {
                return Err(ScheduleError::InvalidCronExpression(
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

    pub fn next_activation_time(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Schedule::TimeDelta(td) => Some(now + td.every),
            Schedule::CronExpression(ce) => ce.upcoming(Utc).next(),
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

#[cfg(test)]
mod tests {
    use chrono::prelude::*;

    use super::*;

    #[test]
    fn test_validate_invalid_expression() {
        // Try to pass invalid cron expression
        let invalid_cron_expression = "invalid".to_string();
        let err_result =
            Schedule::validate_cron_expression(invalid_cron_expression.clone()).unwrap_err();

        assert_eq!(
            err_result.to_string(),
            format!("Cron expression {} is invalid", &invalid_cron_expression),
        );
    }

    #[test]
    fn test_validate_expired_expression() {
        // Try to pass invalid cron expression
        let expired_cron_expression = "0 0 0 1 JAN ? 2024".to_string();
        let err_result =
            Schedule::validate_cron_expression(expired_cron_expression.clone()).unwrap_err();

        assert_eq!(
            err_result.to_string(),
            format!(
                "Cron expression {} iteration has been exceeded",
                &expired_cron_expression
            ),
        );
    }

    #[test]
    fn test_get_next_time_from_expression() {
        let cron_expression: Schedule = "0 0 0 1 JAN ? *".to_string().into();

        let current_year = Utc::now().year();
        let expected_time = Utc
            .with_ymd_and_hms(current_year + 1, 1, 1, 0, 0, 0)
            .unwrap();

        let cron_time = cron_expression.next_activation_time(Utc::now()).unwrap();

        assert_eq!(cron_time, expected_time);
    }

    // Should return None if cron expression has no more iteration
    #[test]
    fn test_get_next_time_from_expired_expression() {
        let cron_expression: Schedule = "0 0 0 1 JAN ? 2024".to_string().into();
        let cron_time = cron_expression.next_activation_time(Utc::now());

        assert_eq!(cron_time, None);
    }
}
