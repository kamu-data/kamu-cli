// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use retry_backoff::{RetryBackoff, RetryBackoffType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct BackgroundAgentHostConfig {
    /// Delay before an agent is restarted, by consecutive restarts. A run
    /// that lasted at least `max_delay` (`min_delay` if uncapped) resets the
    /// count
    pub restart_backoff: RetryBackoff,

    /// How long agents get to finish their current step on a shutdown,
    /// before the ones still running are aborted
    pub shutdown_timeout: Duration,
}

impl Default for BackgroundAgentHostConfig {
    fn default() -> Self {
        Self {
            restart_backoff: RetryBackoff {
                backoff_type: RetryBackoffType::Exponential,
                min_delay: Duration::from_secs(1),
                max_delay: Some(Duration::from_mins(1)),
            },
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
