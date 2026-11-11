// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use time_source::SystemTimeSource;

use crate::controller_runtime::{
    ResourceControllerCandidateOrigin,
    ResourceControllerOutcomeRecorder,
    ResourceControllerPassPlanner,
    ResourceControllerRegistration,
    ResourceProcessAttempt,
    ResourceProcessExecutor,
};
use crate::domain::{ResourceControllerRuntimeConfig, ResourceProcessOutcome};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Drives one registered controller.
///
/// A drain repeats passes until a pass finds nothing to do:
///
/// ```text
/// plan a pass (eligible + extra resources, minus suppressed ones)
///   -> process the candidates, a few at a time
///   -> record each outcome in the bookkeeping
///   -> plan again
/// ```
///
/// Why the drain ends: every outcome makes its resource drop out of the next
/// pass, unless its next step is due at once:
/// - `Done` suppresses an eligible resource until it changes. An extra one ignores `Done`, so it is
///   excluded for the rest of the drain instead.
/// - `RetryAt` in the future and a failure both set `not_before`, which suppresses either kind
///   until then.
/// - `RetryAt` already passed is the controller saying "the next step is due now": the resource is
///   processed again in the next pass, which is how a multi-step reconciliation runs its steps back
///   to back.
pub(crate) struct ResourceControllerLoop {
    registration: ResourceControllerRegistration,
    catalog: dill::Catalog,
    config: Arc<ResourceControllerRuntimeConfig>,
    time_source: Arc<dyn SystemTimeSource>,
    /// Shared by every loop of this controller, so that the agent's loop and
    /// `run_until_idle` never drain at once and double-process a resource
    drain_lock: Arc<tokio::sync::Mutex<()>>,
}

impl ResourceControllerLoop {
    pub fn new(
        registration: ResourceControllerRegistration,
        catalog: dill::Catalog,
        config: Arc<ResourceControllerRuntimeConfig>,
        time_source: Arc<dyn SystemTimeSource>,
        drain_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        Self {
            registration,
            catalog,
            config,
            time_source,
            drain_lock,
        }
    }

    /// Processes pass after pass until no resource needs attention now
    pub async fn drain(&self) -> Result<ResourceControllerDrainReport, InternalError> {
        let _guard = self.drain_lock.lock().await;

        let planner = ResourceControllerPassPlanner::new(
            &self.registration,
            &self.catalog,
            self.config.batch_size.get(),
        );
        let executor =
            ResourceProcessExecutor::new(&self.registration, self.config.process_concurrency.get());
        let recorder = ResourceControllerOutcomeRecorder::new(
            &self.registration,
            &self.catalog,
            &self.config.failure_backoff,
        );

        let mut report = ResourceControllerDrainReport::default();
        let mut finished_extras = HashSet::new();

        loop {
            let pass = planner
                .plan(self.time_source.now(), &finished_extras)
                .await?;

            // Only the last, empty pass tells what is left waiting: an earlier
            // pass may have deferred a resource that a later one processed
            report.earliest_deferred_extra = pass.earliest_deferred_extra();
            if pass.is_empty() {
                return Ok(report);
            }

            for (candidate, attempt) in executor.run(pass.into_candidates()).await {
                report.processed += 1;

                if candidate.origin() == ResourceControllerCandidateOrigin::Extra
                    && matches!(
                        attempt,
                        ResourceProcessAttempt::Processed(ResourceProcessOutcome::Done(_))
                    )
                {
                    finished_extras.insert(candidate.id());
                }

                // A failure to record fails the drain: the loop is then
                // restarted, and the resource processed again
                recorder
                    .record(candidate.id(), &attempt, self.time_source.now())
                    .await?;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What a drain did, and what it left waiting
#[derive(Debug, Default)]
pub(crate) struct ResourceControllerDrainReport {
    processed: usize,
    earliest_deferred_extra: Option<DateTime<Utc>>,
}

impl ResourceControllerDrainReport {
    /// `process` calls made
    pub fn processed(&self) -> usize {
        self.processed
    }

    /// The earliest moment an extra resource left out for its backoff becomes
    /// due. The bookkeeping storage cannot compute it, since it does not know
    /// the extra set.
    pub fn earliest_deferred_extra(&self) -> Option<DateTime<Utc>> {
        self.earliest_deferred_extra
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
