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
use internal_error::{ErrorIntoInternal, InternalError};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents periodic evaluation schedule settings
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Schedule {
    /// Time-delta based schedule
    TimeDelta(ScheduleTimeDelta),
    /// Cron-based schedule
    Cron(ScheduleCron),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleTimeDelta {
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<String>")]
    pub every: chrono::Duration,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleCron {
    pub source_5component_cron_expression: String,
    #[serde_as(as = "DisplayFromStr")]
    pub cron_schedule: cron::Schedule,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ScheduleCronError {
    #[error(transparent)]
    InvalidCronExpression(#[from] InvalidCronExpressionError),

    #[error(transparent)]
    Internal(#[from] InternalError),
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

// Classic CRON expression has 5 components: min hour dayOfMonth month dayOfWeek
const CLASSIC_CRONTAB_COMPONENTS_COUNT: usize = 5;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Schedule {
    pub fn try_from_5component_cron_expression(
        source_5component_cron_expression: &str,
    ) -> Result<Schedule, ScheduleCronError> {
        // Ensure we obtained classic 5-component CRONTAB expression
        let components_count = source_5component_cron_expression.split_whitespace().count();
        if components_count != CLASSIC_CRONTAB_COMPONENTS_COUNT {
            return Err(ScheduleCronError::InvalidCronExpression(
                InvalidCronExpressionError {
                    expression: source_5component_cron_expression.to_string(),
                },
            ));
        }

        // The `cron` crate requires seconds, which we won't use, but have to provide
        let cron_expression_with_sec = format!("0 {source_5component_cron_expression}");
        let Ok(cron_schedule) = cron::Schedule::from_str(&cron_expression_with_sec) else {
            return Err(ScheduleCronError::InvalidCronExpression(
                InvalidCronExpressionError {
                    expression: source_5component_cron_expression.to_string(),
                },
            ));
        };

        // Ensure there is next value - we don't use years, so it should not be possible
        match cron_schedule.upcoming(Utc).next() {
            Some(_) => Ok(Schedule::Cron(ScheduleCron {
                source_5component_cron_expression: source_5component_cron_expression.to_string(),
                cron_schedule,
            })),
            None => Err(ScheduleCronError::Internal(
                CronExpressionIterationError {
                    expression: source_5component_cron_expression.to_string(),
                }
                .int_err(),
            )),
        }
    }

    pub fn next_activation_time(
        &self,
        now: DateTime<Utc>,
        maybe_last_attempt_time: Option<DateTime<Utc>>,
    ) -> DateTime<Utc> {
        match self {
            // For TimeDelta, take the last activation time into account, if any recorded.
            // If we know, the last run was a long time ago or even never happened - no need to
            // wait.
            Schedule::TimeDelta(td) => {
                if let Some(last_attempt_time) = maybe_last_attempt_time {
                    let planned_activation_time = last_attempt_time + td.every;
                    if planned_activation_time < now {
                        now
                    } else {
                        planned_activation_time
                    }
                } else {
                    now
                }
            }
            // CRON expressions do not care of current or last activation time,
            // they always pick next by the CRON expression
            Schedule::Cron(ce) => ce
                .cron_schedule
                .after(&now)
                .next()
                .expect("CRON expressions we allow should never expire"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<chrono::Duration> for Schedule {
    fn from(value: chrono::Duration) -> Self {
        Self::TimeDelta(ScheduleTimeDelta { every: value })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use chrono::prelude::*;

    use super::*;

    #[test]
    fn test_validate_invalid_cron_expression() {
        // Try to pass invalid cron expression
        let invalid_cron_expression = "invalid".to_string();
        let err_result =
            Schedule::try_from_5component_cron_expression(&invalid_cron_expression).unwrap_err();

        assert_eq!(
            err_result.to_string(),
            format!("Cron expression {} is invalid", &invalid_cron_expression),
        );
    }

    #[test]
    fn test_get_next_time_from_cron_expression() {
        let schedule = Schedule::try_from_5component_cron_expression("0 0 1 JAN ?").unwrap();

        let current_year = Utc::now().year();
        let expected_time = Utc
            .with_ymd_and_hms(current_year + 1, 1, 1, 0, 0, 0)
            .unwrap();

        let next_time = schedule.next_activation_time(Utc::now(), None);
        assert_eq!(next_time, expected_time);
    }

    #[test]
    fn test_parse_cron_expression_with_year_fails() {
        let res = Schedule::try_from_5component_cron_expression("0 0 1 JAN ? 2024");
        assert_matches!(res, Err(ScheduleCronError::InvalidCronExpression(_)));
    }

    #[test]
    fn test_parse_cron_expression_with_seconds_fails() {
        let res = Schedule::try_from_5component_cron_expression("0 0 0 1 JAN ?");
        assert_matches!(res, Err(ScheduleCronError::InvalidCronExpression(_)));
    }
}
