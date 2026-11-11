// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use chrono::TimeDelta;
use internal_error::InternalError;
use retry_backoff::RetryBackoff;
use time_source::SystemTimeSource;
use tokio::task::{JoinError, JoinSet};
use wakeup_listener::WakeupListener;

use crate::controller_runtime::{
    ResourceControllerDrainHoldBack,
    ResourceControllerLoop,
    ResourceControllerRuntimeMetrics,
    panic_message,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Keeps one controller's loop running.
///
/// The loop fails when the runtime's own work fails: a query of the
/// bookkeeping storage, recording an outcome, or a call to the controller
/// outside `process`. It may also panic. Either way the supervisor logs it,
/// counts it, and starts the loop again after a backoff that grows with each
/// consecutive failure; a drain and the wait after it, both completed, reset
/// the count. Other controllers' loops are not affected.
pub(crate) struct ResourceControllerSupervisor {
    controller_loop: Arc<ResourceControllerLoop>,
    listener: Arc<dyn WakeupListener>,
    restart_backoff: RetryBackoff,
    time_source: Arc<dyn SystemTimeSource>,
    metrics: Arc<ResourceControllerRuntimeMetrics>,
}

impl ResourceControllerSupervisor {
    /// The listener is kept across restarts, so that no hint is missed
    /// while the loop is down
    pub fn new(
        controller_loop: ResourceControllerLoop,
        listener: Arc<dyn WakeupListener>,
        restart_backoff: RetryBackoff,
        time_source: Arc<dyn SystemTimeSource>,
        metrics: Arc<ResourceControllerRuntimeMetrics>,
    ) -> Self {
        Self {
            controller_loop: Arc::new(controller_loop),
            listener,
            restart_backoff,
            time_source,
            metrics,
        }
    }

    pub async fn run(self) -> Infallible {
        let consecutive_failures = Arc::new(AtomicU32::new(0));
        let controller_name = self
            .controller_loop
            .registration()
            .meta()
            .controller_name
            .clone();

        loop {
            // A task of its own, so that a panic ends the loop, not the
            // supervisor. The set aborts the task if the supervisor is dropped.
            let mut running = JoinSet::new();
            running.spawn(Self::run_loop(
                self.controller_loop.clone(),
                self.listener.clone(),
                consecutive_failures.clone(),
            ));

            let ended = running.join_next().await;
            // Read after the loop ended: a drain that completed reset it
            let failures = consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
            let delay = self.restart_backoff.delay(failures);

            match ended {
                Some(Ok(Err(e))) => tracing::error!(
                    controller = controller_name.as_str(),
                    error = ?e,
                    error_msg = %e,
                    consecutive_failures = failures,
                    ?delay,
                    "Resource controller loop failed, restarting it after a delay"
                ),
                Some(Ok(Ok(never))) => match never {},
                Some(Err(join_error)) => tracing::error!(
                    controller = controller_name.as_str(),
                    panic = %Self::panic_text(join_error),
                    consecutive_failures = failures,
                    ?delay,
                    "Resource controller loop panicked, restarting it after a delay"
                ),
                None => unreachable!("the set holds the loop's task"),
            }
            self.metrics.on_loop_restart(&controller_name);

            self.time_source
                .sleep(TimeDelta::from_std(delay).unwrap_or(TimeDelta::MAX))
                .await;
        }
    }

    /// The task is never aborted while awaited, so it panicked
    fn panic_text(join_error: JoinError) -> String {
        if join_error.is_panic() {
            panic_message(join_error.into_panic().as_ref())
        } else {
            join_error.to_string()
        }
    }

    async fn run_loop(
        controller_loop: Arc<ResourceControllerLoop>,
        listener: Arc<dyn WakeupListener>,
        consecutive_failures: Arc<AtomicU32>,
    ) -> Result<Infallible, InternalError> {
        loop {
            let drained = controller_loop
                .drain(
                    Some(listener.as_ref()),
                    &mut ResourceControllerDrainHoldBack::default(),
                )
                .await?;
            tracing::debug!(
                processed = drained.processed(),
                "Resource controller drain finished"
            );

            controller_loop.wait(listener.as_ref(), &drained).await?;

            // Only a whole cycle counts as healthy: a loop that drains and
            // then fails to wait every time must still back off
            consecutive_failures.store(0, Ordering::Relaxed);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
