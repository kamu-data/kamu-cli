// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchingRule {
    min_records_to_await: u64,
    #[serde_as(as = "serde_with::DurationSeconds<i64>")]
    max_batching_interval: Duration,
}

impl BatchingRule {
    const MAX_BATCHING_INTERVAL_HOURS: i64 = 24;

    pub fn new_checked(
        min_records_to_await: u64,
        max_batching_interval: Duration,
    ) -> Result<Self, BatchingRuleValidationError> {
        if min_records_to_await == 0 {
            return Err(BatchingRuleValidationError::MinRecordsToAwaitNotPositive);
        }

        let lower_interval_bound = Duration::try_seconds(0).unwrap();
        if lower_interval_bound >= max_batching_interval {
            return Err(BatchingRuleValidationError::MinIntervalNotPositive);
        }

        let upper_interval_bound = Duration::try_hours(Self::MAX_BATCHING_INTERVAL_HOURS).unwrap();
        if max_batching_interval > upper_interval_bound {
            return Err(BatchingRuleValidationError::MaxIntervalAboveLimit);
        }

        Ok(Self {
            min_records_to_await,
            max_batching_interval,
        })
    }

    #[inline]
    pub fn min_records_to_await(&self) -> u64 {
        self.min_records_to_await
    }

    #[inline]
    pub fn max_batching_interval(&self) -> &Duration {
        &self.max_batching_interval
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum BatchingRuleValidationError {
    #[error("Minimum records to await must be a positive number")]
    MinRecordsToAwaitNotPositive,

    #[error("Minimum interval to await should be positive")]
    MinIntervalNotPositive,

    #[error(
        "Maximum interval to await should not exceed {} hours",
        BatchingRule::MAX_BATCHING_INTERVAL_HOURS
    )]
    MaxIntervalAboveLimit,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use chrono::TimeDelta;

    use crate::{BatchingRule, BatchingRuleValidationError};

    #[test]
    fn test_good_batching_rule() {
        assert_matches!(
            BatchingRule::new_checked(1, TimeDelta::try_minutes(15).unwrap()),
            Ok(_)
        );
        assert_matches!(
            BatchingRule::new_checked(1_000_000, TimeDelta::try_hours(3).unwrap()),
            Ok(_)
        );
        assert_matches!(
            BatchingRule::new_checked(1, TimeDelta::try_hours(24).unwrap()),
            Ok(_)
        );
    }

    #[test]
    fn test_non_positive_min_records() {
        assert_matches!(
            BatchingRule::new_checked(0, TimeDelta::try_minutes(15).unwrap()),
            Err(BatchingRuleValidationError::MinRecordsToAwaitNotPositive)
        );
    }

    #[test]
    fn test_non_positive_max_interval() {
        assert_matches!(
            BatchingRule::new_checked(1, TimeDelta::try_minutes(0).unwrap()),
            Err(BatchingRuleValidationError::MinIntervalNotPositive)
        );
        assert_matches!(
            BatchingRule::new_checked(1, TimeDelta::try_minutes(-1).unwrap()),
            Err(BatchingRuleValidationError::MinIntervalNotPositive)
        );
    }

    #[test]
    fn test_too_large_max_interval() {
        assert_matches!(
            BatchingRule::new_checked(
                1,
                TimeDelta::try_hours(24).unwrap() + TimeDelta::nanoseconds(1)
            ),
            Err(BatchingRuleValidationError::MaxIntervalAboveLimit)
        );
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
