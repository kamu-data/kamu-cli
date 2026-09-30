// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::num::NonZeroUsize;

use async_utils::BackgroundAgent;
use chrono::{DateTime, Duration, DurationRound, Utc};
use internal_error::{InternalError, ResultIntoInternal};

use crate::RetryPolicy;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_AGENT_NAME: &str = "dev.kamu.domain.flow-system.FlowAgent";

#[async_trait::async_trait]
pub trait FlowAgent: BackgroundAgent {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FlowAgentConfig {
    /// Scheduling granularity: activation times are rounded to it, and failed
    /// activations retried after it. Not a polling period
    pub awaiting_step: Duration,
    /// Defines minimal time between 2 runs of the same flow configuration
    pub mandatory_throttling_period: Duration,
    /// Default retry policy for specific flow types
    pub default_retry_policy_by_flow_type: HashMap<String, RetryPolicy>,
}

impl FlowAgentConfig {
    pub fn round_time(&self, time: DateTime<Utc>) -> Result<DateTime<Utc>, InternalError> {
        let rounded_time = time.duration_round(self.awaiting_step).int_err()?;
        Ok(rounded_time)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FlowAgentActivationConfig {
    /// How many due flows are loaded at once, bounding the memory and the
    /// loading transaction of a pass after downtime
    pub batch_size: NonZeroUsize,
}

impl FlowAgentActivationConfig {
    // Loaded in one transaction, which on Sqlite holds the only connection
    pub fn local_default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(20).unwrap(),
        }
    }

    // Postgres pools connections, so larger pages mostly save round trips
    pub fn production_default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(100).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
