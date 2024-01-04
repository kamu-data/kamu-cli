// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::prelude::*;
use chrono::Utc;
use kamu_flow_system::{
    CronExpressionIterationError,
    InvalidCronExpressionError,
    Schedule,
    ScheduleError,
};

#[test]
fn test_schedule_cron_validation() {
    // Try to pass invalid cron expression
    let invalid_cron_expression = "invalid".to_string();

    assert_matches!(
        Schedule::validate_cron_expression(invalid_cron_expression.clone()),
        Err(ScheduleError::InvalidCronExptression(
            InvalidCronExpressionError {
                expression
            }
        )) if expression == invalid_cron_expression
    );

    // Try to pass valid cron expression and get expected time
    let cron_expression: Schedule = "0 0 0 1 JAN ? *".to_string().into();

    let current_year = Utc::now().year();
    let expected_time = Utc
        .with_ymd_and_hms(current_year + 1, 1, 1, 0, 0, 0)
        .unwrap();

    let cron_time = cron_expression.next_activation_time(Utc::now()).unwrap();

    assert_eq!(cron_time, expected_time);

    // Try to pass valid cron expression by with last iteration in past
    let expired_cron_expression = "0 0 0 1 JAN ? 2024".to_string();

    assert_matches!(
        Schedule::validate_cron_expression(expired_cron_expression.clone()),
        Err(ScheduleError::CronExpressionIterationExceed(
            CronExpressionIterationError {
                expression
            }
        )) if expression == expired_cron_expression
    );
}
