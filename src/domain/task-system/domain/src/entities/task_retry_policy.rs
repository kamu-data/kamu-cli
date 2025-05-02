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
