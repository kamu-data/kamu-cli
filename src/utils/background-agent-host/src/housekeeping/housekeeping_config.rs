// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How each housekeeping job runs, by job name. A job without an entry runs
/// with [`HousekeepingJobConfig::default`].
#[derive(Debug, Clone, Default)]
pub struct HousekeepingConfig {
    pub jobs: BTreeMap<String, HousekeepingJobConfig>,
}

impl HousekeepingConfig {
    pub fn job(&self, job_name: &str) -> HousekeepingJobConfig {
        self.jobs.get(job_name).cloned().unwrap_or_default()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HousekeepingJobConfig {
    /// Whether the job runs, on the host and after CLI commands
    pub enabled: bool,
    /// Items removed per batch
    pub batch_size: usize,
    /// The longest sleep between two passes: a job without a deadline runs
    /// once per interval, and a job with one notices an earlier deadline
    /// within it
    pub interval: Duration,
    /// The pause between two batches of one pass
    pub pause_between_batches: Duration,
}

impl Default for HousekeepingJobConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: 100,
            interval: Duration::from_mins(5),
            pause_between_batches: Duration::from_millis(100),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
