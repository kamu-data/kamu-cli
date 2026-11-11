// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use time_source::SystemTimeSource;
use wakeup_listener::{WakeupListener, WakeupListenerConfig};

use crate::controller_runtime::{
    ResourceControllerDrainHoldBack,
    ResourceControllerOutcomeRecorder,
    ResourceControllerPassPlanner,
    ResourceControllerRegistration,
    ResourceControllerWait,
    ResourceProcessExecutor,
};
use crate::domain::ResourceControllerRuntimeConfig;

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
/// - `Done` suppresses an eligible resource until it changes. An extra one ignores `Done`, so the
///   drain holds it back instead.
/// - `RetryAt` in the future and a failure both set `not_before`, which suppresses either kind
///   until then.
/// - `RetryAt` already passed is the controller saying "the next step is due now": the resource is
///   processed again in the next pass, which is how a multi-step reconciliation runs its steps back
///   to back. A controller that never stops doing so is held back after a few calls
///   (`ResourceControllerDrainHoldBack`).
pub(crate) struct ResourceControllerLoop {
    registration: ResourceControllerRegistration,
    catalog: dill::Catalog,
    config: Arc<ResourceControllerRuntimeConfig>,
    wakeup_config: Arc<WakeupListenerConfig>,
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
        wakeup_config: Arc<WakeupListenerConfig>,
        time_source: Arc<dyn SystemTimeSource>,
        drain_lock: Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        Self {
            registration,
            catalog,
            config,
            wakeup_config,
            time_source,
            drain_lock,
        }
    }

    pub fn registration(&self) -> &ResourceControllerRegistration {
        &self.registration
    }

    /// Processes pass after pass until no resource needs attention now. The
    /// listener, when the agent's loop drains, gets a heartbeat per pass, so a
    /// long drain still proves the loop alive.
    pub async fn drain(
        &self,
        listener: Option<&dyn WakeupListener>,
    ) -> Result<ResourceControllerDrainReport, InternalError> {
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

        let mut processed = 0;
        let mut hold_back = ResourceControllerDrainHoldBack::default();

        loop {
            let planned_at = self.time_source.now();
            let pass = planner.plan(planned_at, &hold_back).await?;

            // Only the last, empty pass tells what is left waiting: an earlier
            // pass may have deferred a resource that a later one processed
            if pass.is_empty() {
                return Ok(ResourceControllerDrainReport {
                    processed,
                    extras_listed_at: planned_at,
                    earliest_deferred_extra: pass.earliest_deferred_extra(),
                    held_back: hold_back.len() > 0,
                });
            }

            for (candidate, attempt) in executor.run(pass.into_candidates()).await {
                processed += 1;
                hold_back.note_attempt(
                    &candidate,
                    &attempt,
                    self.registration.meta().controller_name.as_str(),
                );

                // A failure to record fails the drain: the loop is then
                // restarted, and the resource processed again
                recorder
                    .record(candidate.id(), &attempt, self.time_source.now())
                    .await?;
            }

            if let Some(listener) = listener {
                listener.heartbeat();
            }
        }
    }

    /// Waits until the next drain is due: see `ResourceControllerWait`
    pub async fn wait(
        &self,
        listener: &dyn WakeupListener,
        drained: &ResourceControllerDrainReport,
    ) -> Result<(), InternalError> {
        ResourceControllerWait::new(
            &self.registration,
            &self.catalog,
            &self.wakeup_config,
            self.time_source.as_ref(),
            self.config.failure_backoff.min_delay,
        )
        .wait(listener, drained)
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What a drain did, and what it left waiting
#[derive(Debug)]
pub(crate) struct ResourceControllerDrainReport {
    processed: usize,
    extras_listed_at: DateTime<Utc>,
    earliest_deferred_extra: Option<DateTime<Utc>>,
    held_back: bool,
}

impl ResourceControllerDrainReport {
    /// `process` calls made
    pub fn processed(&self) -> usize {
        self.processed
    }

    /// The moment the last pass listed the extra resources. Those due by then
    /// were processed or deferred; those due later were not considered yet.
    pub fn extras_listed_at(&self) -> DateTime<Utc> {
        self.extras_listed_at
    }

    /// The earliest moment an extra resource left out for its backoff becomes
    /// due. The bookkeeping storage cannot compute it, since it does not know
    /// the extra set.
    pub fn earliest_deferred_extra(&self) -> Option<DateTime<Utc>> {
        self.earliest_deferred_extra
    }

    /// Whether the drain held back resources that are still due
    pub fn held_back(&self) -> bool {
        self.held_back
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
