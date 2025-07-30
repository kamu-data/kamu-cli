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

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowRetryPolicy {
    pub max_attempts: u32,
    pub min_delay: TimeDelta,
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

#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
#[graphql(remote = "kamu_flow_system::RetryBackoffType")]
pub enum FlowRetryBackoffType {
    Fixed,
    Linear,
    Exponential,
    ExponentialWithJitter,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct FlowRetryPolicyInput {
    pub max_attempts: u32,
    pub min_delay: TimeDeltaInput,
    pub backoff_type: FlowRetryBackoffType,
}

impl From<FlowRetryPolicyInput> for kamu_flow_system::RetryPolicy {
    fn from(value: FlowRetryPolicyInput) -> Self {
        let duration: chrono::Duration = value.min_delay.into();
        let min_delay_seconds = duration.num_seconds();

        Self {
            max_attempts: value.max_attempts,
            min_delay_seconds: u32::try_from(min_delay_seconds).unwrap(),
            backoff_type: value.backoff_type.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
