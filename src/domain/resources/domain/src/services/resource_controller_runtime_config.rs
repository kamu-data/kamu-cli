// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::time::Duration;

use retry_backoff::{RetryBackoff, RetryBackoffType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceControllerRuntimeConfig {
    /// Candidates a drain pass loads at once, for each controller
    pub batch_size: NonZeroUsize,

    /// `process` calls running at once, for each controller
    pub process_concurrency: NonZeroUsize,

    /// Delay before a resource whose processing failed is asked about again,
    /// by consecutive failures
    pub failure_backoff: RetryBackoff,

    /// Delay before a failed controller loop is restarted, by consecutive
    /// failures
    pub loop_restart_backoff: RetryBackoff,
}

impl Default for ResourceControllerRuntimeConfig {
    fn default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(20).unwrap(),
            process_concurrency: NonZeroUsize::MIN,
            failure_backoff: RetryBackoff {
                backoff_type: RetryBackoffType::Exponential,
                min_delay: Duration::from_secs(1),
                max_delay: Some(Duration::from_mins(5)),
            },
            loop_restart_backoff: RetryBackoff {
                backoff_type: RetryBackoffType::Exponential,
                min_delay: Duration::from_secs(1),
                max_delay: Some(Duration::from_mins(1)),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
