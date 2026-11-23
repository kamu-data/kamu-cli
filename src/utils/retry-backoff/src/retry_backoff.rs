// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use crate::RetryBackoffType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The delay before each retry: a backoff function of the attempt number,
/// optionally capped
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct RetryBackoff {
    pub backoff_type: RetryBackoffType,
    pub min_delay: Duration,
    pub max_delay: Option<Duration>,
}

impl RetryBackoff {
    /// The delay before the given retry, counting from 1; 0 is treated as 1.
    /// Saturates instead of overflowing, then applies the cap.
    pub fn delay(&self, attempt: u32) -> Duration {
        let attempt = attempt.max(1);
        let exponential_delay = || {
            self.min_delay
                .saturating_mul(2u32.saturating_pow(attempt - 1))
        };

        let delay = match self.backoff_type {
            RetryBackoffType::Fixed => self.min_delay,
            RetryBackoffType::Linear => self.min_delay.saturating_mul(attempt),
            RetryBackoffType::Exponential => exponential_delay(),
            RetryBackoffType::ExponentialWithJitter => {
                exponential_delay().saturating_add(self.jitter())
            }
        };

        match self.max_delay {
            Some(max_delay) => delay.min(max_delay),
            None => delay,
        }
    }

    /// A random duration in `[0, min_delay)`, empty when the delay is 0
    fn jitter(&self) -> Duration {
        let min_delay_nanos = u64::try_from(self.min_delay.as_nanos()).unwrap_or(u64::MAX);
        let jitter_nanos = rand::random::<u64>()
            .checked_rem(min_delay_nanos)
            .unwrap_or(0);
        Duration::from_nanos(jitter_nanos)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    fn backoff(backoff_type: RetryBackoffType, max_delay: Option<u64>) -> RetryBackoff {
        RetryBackoff {
            backoff_type,
            min_delay: Duration::from_secs(10),
            max_delay: max_delay.map(Duration::from_secs),
        }
    }

    fn delays_secs(backoff: &RetryBackoff) -> Vec<u64> {
        (0..=5)
            .map(|attempt| backoff.delay(attempt).as_secs())
            .collect()
    }

    #[test]
    fn test_delay_per_backoff_type() {
        assert_eq!(
            delays_secs(&backoff(RetryBackoffType::Fixed, None)),
            [10, 10, 10, 10, 10, 10]
        );
        assert_eq!(
            delays_secs(&backoff(RetryBackoffType::Linear, None)),
            [10, 10, 20, 30, 40, 50]
        );
        assert_eq!(
            delays_secs(&backoff(RetryBackoffType::Exponential, None)),
            [10, 10, 20, 40, 80, 160]
        );
    }

    #[test]
    fn test_jitter_stays_below_the_minimal_delay() {
        let backoff = backoff(RetryBackoffType::ExponentialWithJitter, None);

        for attempt in 1..=4 {
            let exponential = Duration::from_secs(10 * 2u64.pow(attempt - 1));
            let delay = backoff.delay(attempt);
            assert!(delay >= exponential, "{attempt}: {delay:?}");
            assert!(
                delay < exponential + Duration::from_secs(10),
                "{attempt}: {delay:?}"
            );
        }
    }

    #[test]
    fn test_delay_is_capped() {
        assert_eq!(
            delays_secs(&backoff(RetryBackoffType::Exponential, Some(45))),
            [10, 10, 20, 40, 45, 45]
        );
        assert_eq!(
            backoff(RetryBackoffType::ExponentialWithJitter, Some(45))
                .delay(10)
                .as_secs(),
            45
        );
    }

    #[test]
    fn test_delay_saturates() {
        let backoff = RetryBackoff {
            backoff_type: RetryBackoffType::Exponential,
            min_delay: Duration::MAX,
            max_delay: None,
        };

        assert_eq!(backoff.delay(u32::MAX), Duration::MAX);
    }

    #[test]
    fn test_zero_min_delay() {
        for backoff_type in [
            RetryBackoffType::Fixed,
            RetryBackoffType::Linear,
            RetryBackoffType::Exponential,
            RetryBackoffType::ExponentialWithJitter,
        ] {
            let backoff = RetryBackoff {
                backoff_type,
                min_delay: Duration::ZERO,
                max_delay: None,
            };

            assert_eq!(backoff.delay(3), Duration::ZERO, "{backoff_type:?}");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
