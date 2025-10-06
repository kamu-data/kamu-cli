// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use async_utils::BackgroundAgent;
use chrono::{DateTime, DurationRound, Utc};
use internal_error::{InternalError, ResultIntoInternal};

use crate::RetryPolicy;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowAgent: BackgroundAgent {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FlowAgentConfig {
    /// Defines discretion for main scheduling loop: how often new data is
    /// checked and processed
    pub awaiting_step: chrono::Duration,
    /// Defines minimal time between 2 runs of the same flow configuration
    pub mandatory_throttling_period: chrono::Duration,
    /// Default retry policy for specific flow types
    pub default_retry_policy_by_flow_type: HashMap<String, RetryPolicy>,
}

impl FlowAgentConfig {
    pub fn new(
        awaiting_step: chrono::Duration,
        mandatory_throttling_period: chrono::Duration,
        default_retry_policy_by_flow_type: HashMap<String, RetryPolicy>,
    ) -> Self {
        Self {
            awaiting_step,
            mandatory_throttling_period,
            default_retry_policy_by_flow_type,
        }
    }

    pub fn test_default() -> Self {
        Self::new(
            chrono::Duration::seconds(1),
            chrono::Duration::minutes(1),
            HashMap::new(),
        )
    }

    pub fn round_time(&self, time: DateTime<Utc>) -> Result<DateTime<Utc>, InternalError> {
        let rounded_time = time.duration_round(self.awaiting_step).int_err()?;
        Ok(rounded_time)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
