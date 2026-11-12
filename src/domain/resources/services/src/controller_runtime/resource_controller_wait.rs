// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use database_common::DatabaseTransactionRunner;
use internal_error::{InternalError, ResultIntoInternal};
use time_source::SystemTimeSource;
use wakeup_listener::{WakeHint, WakeupListener, WakeupListenerConfig};

use crate::controller_runtime::{ResourceControllerDrainReport, ResourceControllerRegistration};
use crate::domain::ResourceControllerStateRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How a controller loop waits between two drains.
///
/// The wait ends at whichever comes first:
///
/// | Wakeup | Source |
/// | --- | --- |
/// | A hint for the controller's schema | the lifecycle message consumer, through `ResourceControllerHints` |
/// | The effective deadline | the earliest moment a resource becomes due and is no longer suppressed |
/// | The fallback timeout | `maxListeningTimeout`: a safety net for a missed hint |
///
/// The effective deadline is the earliest of these moments, because no
/// single store knows them all:
///
/// - the earliest `not_before` of a resource the eligibility predicates select, from the
///   bookkeeping storage (a retry, or the end of a backoff);
/// - the controller's `next_extra_deadline` after the last listing of extra resources. One due by
///   then belongs to extra resources the drain saw: processed, or left out for their backoff. One
///   due after it, even if already passed by the time the wait starts, was not considered and makes
///   the loop drain again at once;
/// - the earliest `not_before` among extra resources the last pass left out. This is what keeps an
///   extra resource that keeps failing, and whose own due time passed long ago, from waking the
///   loop again and again during its backoff.
///
/// After a drain that held resources back, the loop pauses for a fixed
/// delay instead, which no hint ends.
///
/// The deadline is re-read after every drain, so it needs no timers: a
/// resource scheduled earlier than the awaited moment comes with a hint, and
/// one postponed is found not due when the loop wakes.
pub(crate) struct ResourceControllerWait<'a> {
    registration: &'a ResourceControllerRegistration,
    catalog: &'a dill::Catalog,
    wakeup_config: &'a WakeupListenerConfig,
    time_source: &'a dyn SystemTimeSource,
    /// How long to pause after a drain that held resources back
    held_back_delay: Duration,
}

impl<'a> ResourceControllerWait<'a> {
    pub fn new(
        registration: &'a ResourceControllerRegistration,
        catalog: &'a dill::Catalog,
        wakeup_config: &'a WakeupListenerConfig,
        time_source: &'a dyn SystemTimeSource,
        held_back_delay: Duration,
    ) -> Self {
        Self {
            registration,
            catalog,
            wakeup_config,
            time_source,
            held_back_delay,
        }
    }

    /// Waits until the next drain is due
    pub async fn wait(
        &self,
        listener: &dyn WakeupListener,
        drained: &ResourceControllerDrainReport,
    ) -> Result<(), InternalError> {
        // The drain held back resources whose controller never settles, which
        // are due again at once by definition. Pausing keeps them from
        // spinning the loop, and no hint ends the pause: such a controller
        // writes status on every call, which hints its own loop. A hint that
        // arrives meanwhile is kept for the next wait.
        if drained.held_back() {
            let pause = chrono::Duration::from_std(self.held_back_delay).int_err()?;
            self.time_source.sleep(pause).await;
            tracing::debug!(
                ?pause,
                "Resource controller paused for the resources it held back"
            );
            return Ok(());
        }

        let deadline = self.effective_deadline(drained).await?;
        // Read after the queries, so that their time is not slept again
        let now = self.time_source.now();

        let wait_for_hint = listener.wait_wake(
            self.wakeup_config.max_listening_timeout,
            self.wakeup_config.min_debounce_interval,
        );

        let Some(deadline) = deadline else {
            let hint = wait_for_hint.await?;
            tracing::debug!(?hint, "Resource controller woke up");
            return Ok(());
        };

        // Passed: a resource became due after the last pass looked, so the
        // loop drains again at once
        let Ok(sleep_for) = (deadline - now).to_std() else {
            return Ok(());
        };
        if sleep_for.is_zero() {
            return Ok(());
        }
        let sleep_for = chrono::Duration::from_std(sleep_for).int_err()?;

        tokio::select! {
            hint = wait_for_hint => {
                tracing::debug!(hint = ?hint?, "Resource controller woke up");
            }
            () = self.time_source.sleep(sleep_for) => {
                tracing::debug!(hint = ?WakeHint::Timeout, %deadline, "Resource controller woke up at its deadline");
            }
        }

        Ok(())
    }

    /// The earliest moment a resource becomes due and is no longer suppressed
    pub async fn effective_deadline(
        &self,
        drained: &ResourceControllerDrainReport,
    ) -> Result<Option<DateTime<Utc>>, InternalError> {
        let query = self.registration.actionable_query();
        let stored = DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |repository: Arc<dyn ResourceControllerStateRepository>| async move {
                    repository.find_next_deadline(query).await
                },
            )
            .await?;

        let extra = self
            .registration
            .controller()
            .next_extra_deadline(drained.extras_listed_at())
            .await?;

        Ok([stored, extra, drained.earliest_deferred_extra()]
            .into_iter()
            .flatten()
            .min())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
