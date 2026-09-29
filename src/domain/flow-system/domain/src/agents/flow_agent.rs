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
use chrono::{DateTime, DurationRound, Utc};
use internal_error::{InternalError, ResultIntoInternal};

use crate::RetryPolicy;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_AGENT_NAME: &str = "dev.kamu.domain.flow-system.FlowAgent";

#[async_trait::async_trait]
pub trait FlowAgent: BackgroundAgent {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FlowAgentConfig {
    /// Scheduling granularity: activation times are rounded to it.
    /// Also the delay before retrying flows whose activation failed.
    /// Not a polling period: the agent wakes up at activation moments
    /// and on flow activation signals
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
    /// How many due flows are activated at once. Each activation is its own
    /// transaction, holding a pooled connection until it commits
    pub concurrency: NonZeroUsize,
}

impl FlowAgentActivationConfig {
    // Sqlite has a single connection: concurrent activations would only queue
    // for it, risking acquire timeouts
    pub fn local_default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(20).unwrap(),
            concurrency: NonZeroUsize::MIN,
        }
    }

    // Postgres pools connections: activations run in parallel, well below the
    // default pool size, which API requests and other agents share
    pub fn production_default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(100).unwrap(),
            concurrency: NonZeroUsize::new(8).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
