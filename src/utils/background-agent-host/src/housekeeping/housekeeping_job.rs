// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A periodic job that removes data nobody needs any more, such as resources
/// past their retention.
///
/// A job runs as an agent of its own on the host ([`crate::HousekeepingAgent`])
/// and, after a CLI command, one batch at a time
/// ([`crate::BackgroundWorkFinalizer`]). Every process may run it at once, so a
/// batch must be idempotent and safe alongside another runner of the same job:
/// two runners never remove an item twice, and a batch cut short by a crash
/// loses nothing.
#[async_trait::async_trait]
pub trait HousekeepingJob: Send + Sync {
    /// A stable name: the agent name, the `job` label of the housekeeping
    /// metrics and the key of the job's configuration
    fn job_name(&self) -> &'static str;

    /// Removes at most `batch_size` items due at `now`, in short transactions
    /// of its own
    async fn run_batch(
        &self,
        now: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<HousekeepingBatchReport, InternalError>;

    /// The moment after `now` when an item next falls due, or `None` for a job
    /// without deadlines of its own, which runs once per interval
    async fn next_due(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What one batch of a housekeeping job did
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HousekeepingBatchReport {
    /// Items the batch removed, by a kind the job defines, such as the
    /// resource type
    pub removed_by_kind: BTreeMap<String, u64>,
    /// Items still due once the batch ended
    pub backlog: u64,
}

impl HousekeepingBatchReport {
    pub fn removed(&self) -> u64 {
        self.removed_by_kind.values().sum()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
