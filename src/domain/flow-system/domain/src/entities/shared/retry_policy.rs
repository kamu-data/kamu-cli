// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Duration, Utc};
use retry_backoff::RetryBackoff;
pub use retry_backoff::RetryBackoffType;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub min_delay_seconds: u32,
    pub backoff_type: RetryBackoffType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl RetryPolicy {
    pub fn new(max_attempts: u32, min_delay_seconds: u32, backoff_type: RetryBackoffType) -> Self {
        // Validation?
        Self {
            max_attempts,
            min_delay_seconds,
            backoff_type,
        }
    }

    pub fn next_attempt_at(
        &self,
        run_attempt: u32,
        last_attempt_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        // The first attempt is not a retry
        assert!(run_attempt > 0);

        // Check retry limit
        if run_attempt > self.max_attempts {
            return None;
        }

        // The delay saturates at u32::MAX seconds
        let delay = RetryBackoff {
            backoff_type: self.backoff_type,
            min_delay: std::time::Duration::from_secs(u64::from(self.min_delay_seconds)),
            max_delay: Some(std::time::Duration::from_secs(u64::from(u32::MAX))),
        }
        .delay(run_attempt);

        Some(last_attempt_at + Duration::from_std(delay).unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    // Delays per backoff type are tested on `RetryBackoff`
    #[test]
    fn test_next_attempt_at_follows_the_backoff_up_to_max_attempts() {
        let policy = RetryPolicy::new(2, 10, RetryBackoffType::Exponential);
        let last_attempt_at = Utc::now();

        assert_eq!(
            policy.next_attempt_at(1, last_attempt_at),
            Some(last_attempt_at + Duration::seconds(10))
        );
        assert_eq!(
            policy.next_attempt_at(2, last_attempt_at),
            Some(last_attempt_at + Duration::seconds(20))
        );
        assert_eq!(policy.next_attempt_at(3, last_attempt_at), None);
    }

    #[test]
    fn test_next_attempt_at_saturates_on_overflow() {
        let last_attempt_at = Utc::now();
        let max_delay = Some(last_attempt_at + Duration::seconds(i64::from(u32::MAX)));

        let policy = RetryPolicy::new(100, 10, RetryBackoffType::Exponential);
        assert_eq!(policy.next_attempt_at(40, last_attempt_at), max_delay);

        let policy = RetryPolicy::new(100, 10, RetryBackoffType::ExponentialWithJitter);
        assert_eq!(policy.next_attempt_at(40, last_attempt_at), max_delay);

        let policy = RetryPolicy::new(2, u32::MAX, RetryBackoffType::Linear);
        assert_eq!(policy.next_attempt_at(2, last_attempt_at), max_delay);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
