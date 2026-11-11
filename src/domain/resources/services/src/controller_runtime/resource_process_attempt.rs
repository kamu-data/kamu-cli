// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, TimeDelta, Utc};
use retry_backoff::RetryBackoff;

use crate::domain::{
    ResourceControllerMeta,
    ResourceControllerState,
    ResourceID,
    ResourceProcessOutcome,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How one `process` call ended
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResourceProcessAttempt {
    Processed(ResourceProcessOutcome),
    /// An error or a panic, as text
    Failed(String),
}

impl ResourceProcessAttempt {
    /// How the call ended, without its details
    pub fn kind(&self) -> ResourceProcessAttemptKind {
        match self {
            Self::Processed(ResourceProcessOutcome::Done(_)) => ResourceProcessAttemptKind::Done,
            Self::Processed(ResourceProcessOutcome::RetryAt(_)) => {
                ResourceProcessAttemptKind::RetryAt
            }
            Self::Failed(_) => ResourceProcessAttemptKind::Failed,
        }
    }

    /// The bookkeeping row recording this attempt. A failure keeps what the
    /// controller handled before and retries after a backoff that grows with
    /// each consecutive failure.
    pub fn next_state(
        &self,
        meta: &ResourceControllerMeta,
        resource_id: ResourceID,
        previous: Option<&ResourceControllerState>,
        now: DateTime<Utc>,
        failure_backoff: &RetryBackoff,
    ) -> ResourceControllerState {
        let previous_handled_generation = previous.and_then(|state| state.handled_generation);
        let previous_handled_status_generation =
            previous.and_then(|state| state.handled_status_generation);

        let (handled_generation, handled_status_generation, not_before, attempts, last_error) =
            match self {
                Self::Processed(ResourceProcessOutcome::Done(version)) => (
                    Some(version.generation),
                    Some(version.status_generation),
                    None,
                    0,
                    None,
                ),
                Self::Processed(ResourceProcessOutcome::RetryAt(at)) => (
                    previous_handled_generation,
                    previous_handled_status_generation,
                    Some(*at),
                    0,
                    None,
                ),
                Self::Failed(error) => {
                    let attempts = previous.map_or(0, |state| state.attempts).saturating_add(1);
                    let delay = TimeDelta::from_std(failure_backoff.delay(attempts))
                        .unwrap_or(TimeDelta::MAX);
                    (
                        previous_handled_generation,
                        previous_handled_status_generation,
                        Some(
                            now.checked_add_signed(delay)
                                .unwrap_or(DateTime::<Utc>::MAX_UTC),
                        ),
                        attempts,
                        Some(error.clone()),
                    )
                }
            };

        ResourceControllerState {
            controller: meta.controller_name.clone(),
            resource_id,
            controller_revision: meta.controller_revision,
            handled_generation,
            handled_status_generation,
            not_before,
            attempts,
            last_error,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How one `process` call ended. Its strings are the `outcome` label of
/// `resource_controller_processed_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum ResourceProcessAttemptKind {
    Done,
    RetryAt,
    Failed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::TimeZone;
    use pretty_assertions::assert_eq;
    use retry_backoff::RetryBackoffType;

    use super::*;
    use crate::domain::{ResourceControllerName, ResourceVersion};

    const META: ResourceControllerMeta = ResourceControllerMeta {
        schema: "https://test.kamu.dev/schemas/test/v1/TestResource",
        controller_name: ResourceControllerName::new("test-controller"),
        controller_revision: 3,
        reacts_to_status: false,
    };

    const BACKOFF: RetryBackoff = RetryBackoff {
        backoff_type: RetryBackoffType::Exponential,
        min_delay: Duration::from_secs(10),
        max_delay: Some(Duration::from_secs(60)),
    };

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap()
    }

    fn failed_twice(resource_id: ResourceID) -> ResourceControllerState {
        ResourceControllerState {
            controller: META.controller_name.clone(),
            resource_id,
            controller_revision: 2,
            handled_generation: Some(4),
            handled_status_generation: Some(7),
            not_before: Some(now()),
            attempts: 2,
            last_error: Some("boom".to_string()),
        }
    }

    #[test]
    fn test_attempt_kind_label_values() {
        use strum::IntoEnumIterator as _;

        let labels: Vec<&'static str> =
            ResourceProcessAttemptKind::iter().map(Into::into).collect();

        assert_eq!(labels, ["done", "retry_at", "failed"]);
    }

    #[test]
    fn test_done_records_the_version_and_clears_the_failures() {
        let resource_id = ResourceID::new(uuid::Uuid::from_u128(1));
        let attempt =
            ResourceProcessAttempt::Processed(ResourceProcessOutcome::Done(ResourceVersion {
                generation: 5,
                status_generation: 9,
            }));

        let state = attempt.next_state(
            &META,
            resource_id,
            Some(&failed_twice(resource_id)),
            now(),
            &BACKOFF,
        );

        assert_eq!(
            state,
            ResourceControllerState {
                controller: META.controller_name.clone(),
                resource_id,
                controller_revision: 3,
                handled_generation: Some(5),
                handled_status_generation: Some(9),
                not_before: None,
                attempts: 0,
                last_error: None,
            }
        );
    }

    #[test]
    fn test_retry_at_keeps_the_handled_version() {
        let resource_id = ResourceID::new(uuid::Uuid::from_u128(2));
        let at = now() + TimeDelta::minutes(5);
        let attempt = ResourceProcessAttempt::Processed(ResourceProcessOutcome::RetryAt(at));

        let state = attempt.next_state(
            &META,
            resource_id,
            Some(&failed_twice(resource_id)),
            now(),
            &BACKOFF,
        );

        assert_eq!(
            (
                state.handled_generation,
                state.handled_status_generation,
                state.not_before,
                state.attempts,
                state.last_error,
                state.controller_revision
            ),
            (Some(4), Some(7), Some(at), 0, None, 3)
        );
    }

    #[test]
    fn test_failure_backs_off_by_consecutive_failures() {
        let resource_id = ResourceID::new(uuid::Uuid::from_u128(3));
        let attempt = ResourceProcessAttempt::Failed("again".to_string());

        let first = attempt.next_state(&META, resource_id, None, now(), &BACKOFF);
        let third = attempt.next_state(
            &META,
            resource_id,
            Some(&failed_twice(resource_id)),
            now(),
            &BACKOFF,
        );

        assert_eq!(
            (
                first.not_before,
                first.attempts,
                first.handled_generation,
                first.last_error.as_deref()
            ),
            (Some(now() + TimeDelta::seconds(10)), 1, None, Some("again"))
        );
        assert_eq!(
            (
                third.not_before,
                third.attempts,
                third.handled_generation,
                third.handled_status_generation
            ),
            (Some(now() + TimeDelta::seconds(40)), 3, Some(4), Some(7))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
