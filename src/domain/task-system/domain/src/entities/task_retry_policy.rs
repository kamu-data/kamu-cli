// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRetryPolicy {
    pub max_retry_attempts: u32,
    pub min_delay_seconds: u32,
    pub backoff_type: TaskRetryBackoffType,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskRetryBackoffType {
    Fixed,
    Linear,
    Exponential,
    ExponentialWithJitter,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskRetryPolicy {
    pub fn new(
        max_retry_attempts: u32,
        min_delay_seconds: u32,
        backoff_type: TaskRetryBackoffType,
    ) -> Self {
        Self {
            max_retry_attempts,
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
        if run_attempt > self.max_retry_attempts {
            return None;
        }

        // Compute delay, depending on the backoff type
        let delay_seconds = match self.backoff_type {
            TaskRetryBackoffType::Fixed => self.min_delay_seconds,
            TaskRetryBackoffType::Linear => self.min_delay_seconds * run_attempt,
            TaskRetryBackoffType::Exponential => {
                self.min_delay_seconds * (2u32.pow(run_attempt - 1))
            }
            TaskRetryBackoffType::ExponentialWithJitter => {
                let jitter = rand::random::<u32>() % self.min_delay_seconds;
                self.min_delay_seconds * (2u32.pow(run_attempt - 1)) + jitter
            }
        };

        Some(last_attempt_at + Duration::seconds(i64::from(delay_seconds)))
    }
}

impl Default for TaskRetryPolicy {
    fn default() -> Self {
        Self {
            max_retry_attempts: 0,
            min_delay_seconds: 0,
            backoff_type: TaskRetryBackoffType::Fixed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_attempt_at_fixed_backoff() {
        let policy = TaskRetryPolicy::new(4, 10, TaskRetryBackoffType::Fixed);
        let last_attempt_at = Utc::now();

        let next_attempt = policy.next_attempt_at(1, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(10)));

        let next_attempt = policy.next_attempt_at(2, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(10)));

        let next_attempt = policy.next_attempt_at(3, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(10)));

        let next_attempt = policy.next_attempt_at(4, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(10)));

        let next_attempt = policy.next_attempt_at(5, last_attempt_at);
        assert_eq!(next_attempt, None); // Exceeds max_retry_attempts
    }

    #[test]
    fn test_next_attempt_at_linear_backoff() {
        let policy = TaskRetryPolicy::new(4, 10, TaskRetryBackoffType::Linear);
        let last_attempt_at = Utc::now();

        let next_attempt = policy.next_attempt_at(1, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(10)));

        let next_attempt = policy.next_attempt_at(2, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(20)));

        let next_attempt = policy.next_attempt_at(3, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(30)));

        let next_attempt = policy.next_attempt_at(4, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(40)));

        let next_attempt = policy.next_attempt_at(5, last_attempt_at);
        assert_eq!(next_attempt, None); // Exceeds max_retry_attempts
    }

    #[test]
    fn test_next_attempt_at_exponential_backoff() {
        let policy = TaskRetryPolicy::new(4, 10, TaskRetryBackoffType::Exponential);
        let last_attempt_at = Utc::now();

        let next_attempt = policy.next_attempt_at(1, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(10)));

        let next_attempt = policy.next_attempt_at(2, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(20)));

        let next_attempt = policy.next_attempt_at(3, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(40)));

        let next_attempt = policy.next_attempt_at(4, last_attempt_at);
        assert_eq!(next_attempt, Some(last_attempt_at + Duration::seconds(80)));

        let next_attempt = policy.next_attempt_at(5, last_attempt_at);
        assert_eq!(next_attempt, None); // Exceeds max_retry_attempts
    }

    #[test]
    fn test_next_attempt_at_exponential_with_jitter_backoff() {
        let policy = TaskRetryPolicy::new(4, 10, TaskRetryBackoffType::ExponentialWithJitter);
        let last_attempt_at = Utc::now();

        let next_attempt = policy.next_attempt_at(1, last_attempt_at);
        assert!(next_attempt.is_some());
        assert!(next_attempt.unwrap() >= last_attempt_at + Duration::seconds(10));
        assert!(next_attempt.unwrap() <= last_attempt_at + Duration::seconds(20));

        let next_attempt = policy.next_attempt_at(2, last_attempt_at);
        assert!(next_attempt.is_some());
        assert!(next_attempt.unwrap() >= last_attempt_at + Duration::seconds(20));
        assert!(next_attempt.unwrap() <= last_attempt_at + Duration::seconds(30));

        let next_attempt = policy.next_attempt_at(3, last_attempt_at);
        assert!(next_attempt.is_some());
        assert!(next_attempt.unwrap() >= last_attempt_at + Duration::seconds(40));
        assert!(next_attempt.unwrap() <= last_attempt_at + Duration::seconds(50));

        let next_attempt = policy.next_attempt_at(4, last_attempt_at);
        assert!(next_attempt.is_some());
        assert!(next_attempt.unwrap() >= last_attempt_at + Duration::seconds(80));
        assert!(next_attempt.unwrap() <= last_attempt_at + Duration::seconds(90));

        let next_attempt = policy.next_attempt_at(5, last_attempt_at);
        assert_eq!(next_attempt, None); // Exceeds max_retry_attempts
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
