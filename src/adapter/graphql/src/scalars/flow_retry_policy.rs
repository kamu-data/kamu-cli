// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How a flow retries after a recoverable failure
#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowRetryPolicy {
    /// Maximum number of retries after the first attempt
    pub max_attempts: u32,
    /// Base delay before a retry
    pub min_delay: TimeDelta,
    /// How the delay grows with each retry
    pub backoff_type: FlowRetryBackoffType,
}

impl From<kamu_flow_system::RetryPolicy> for FlowRetryPolicy {
    fn from(value: kamu_flow_system::RetryPolicy) -> Self {
        let min_delay_duration = chrono::Duration::seconds(i64::from(value.min_delay_seconds));

        Self {
            max_attempts: value.max_attempts,
            min_delay: min_delay_duration.into(),
            backoff_type: value.backoff_type.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How the retry delay grows with each retry
#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
#[graphql(remote = "kamu_flow_system::RetryBackoffType")]
pub enum FlowRetryBackoffType {
    /// Every retry waits the base delay
    Fixed,
    /// Retry N waits N times the base delay
    Linear,
    /// The delay doubles with each retry
    Exponential,
    /// The delay doubles with each retry, plus a random part below the base
    /// delay
    ExponentialWithJitter,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How a flow retries after a recoverable failure
#[derive(InputObject)]
pub struct FlowRetryPolicyInput {
    /// Maximum number of retries after the first attempt
    pub max_attempts: u32,
    /// Base delay before a retry
    pub min_delay: TimeDeltaInput,
    /// How the delay grows with each retry
    pub backoff_type: FlowRetryBackoffType,
}

impl TryFrom<FlowRetryPolicyInput> for kamu_flow_system::RetryPolicy {
    type Error = String;

    fn try_from(value: FlowRetryPolicyInput) -> std::result::Result<Self, Self::Error> {
        let duration: chrono::Duration = value.min_delay.into();
        let min_delay_seconds = u32::try_from(duration.num_seconds()).map_err(|_| {
            format!(
                "Retry policy minimum delay must not exceed {} seconds",
                u32::MAX
            )
        })?;

        Ok(Self {
            max_attempts: value.max_attempts,
            min_delay_seconds,
            backoff_type: value.backoff_type.into(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
