// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_utils::join_error_message;
use chrono::{DateTime, TimeDelta, Utc};
use internal_error::InternalError;
use retry_backoff::RetryBackoff;
use time_source::SystemTimeSource;
use tokio::task::JoinSet;

use crate::{
    BackgroundAgent,
    BackgroundAgentHostMetrics,
    BackgroundAgentRestartReason,
    CancellationToken,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Keeps one agent running until a shutdown.
///
/// A run that fails, panics or returns before a shutdown is logged, counted
/// and restarted after a backoff growing with consecutive restarts; a run that
/// lasted at least the backoff's maximum delay resets the count. A failure or
/// panic of an agent whose failure is fatal cancels the shutdown token
/// instead, so that the host stops every agent. Cancelling the abort token
/// aborts a run in progress and returns once its task has ended.
pub(crate) struct BackgroundAgentSupervisor {
    agent: Arc<dyn BackgroundAgent>,
    restart_backoff: RetryBackoff,
    time_source: Arc<dyn SystemTimeSource>,
    metrics: Arc<BackgroundAgentHostMetrics>,
}

impl BackgroundAgentSupervisor {
    pub fn new(
        agent: Arc<dyn BackgroundAgent>,
        restart_backoff: RetryBackoff,
        time_source: Arc<dyn SystemTimeSource>,
        metrics: Arc<BackgroundAgentHostMetrics>,
    ) -> Self {
        Self {
            agent,
            restart_backoff,
            time_source,
            metrics,
        }
    }

    /// Returns once the agent stopped on a shutdown, or with the error of a
    /// fatal failure
    pub async fn run(
        self,
        shutdown: CancellationToken,
        abort: CancellationToken,
    ) -> Result<(), InternalError> {
        let agent_name = self.agent.agent_name();
        let mut consecutive_restarts = 0;

        loop {
            let started_at = self.time_source.now();
            tracing::info!(agent = agent_name, "Starting background agent");
            self.metrics.set_running(agent_name, true);
            let ended = self.run_once(shutdown.clone(), &abort).await;
            self.metrics.set_running(agent_name, false);

            let reason = match ended {
                AgentRunEnd::Aborted => {
                    tracing::warn!(
                        agent = agent_name,
                        "Background agent did not stop in time and was aborted"
                    );
                    return Ok(());
                }
                AgentRunEnd::Returned if shutdown.is_cancelled() => {
                    tracing::info!(agent = agent_name, "Background agent stopped");
                    return Ok(());
                }
                AgentRunEnd::Returned => {
                    tracing::warn!(
                        agent = agent_name,
                        "Background agent returned before a shutdown"
                    );
                    BackgroundAgentRestartReason::Completed
                }
                AgentRunEnd::Failed(e) => {
                    tracing::error!(
                        agent = agent_name,
                        error = ?e,
                        error_msg = %e,
                        "Background agent failed"
                    );
                    self.on_failure(agent_name, &shutdown, e)?;
                    BackgroundAgentRestartReason::Failed
                }
                AgentRunEnd::Panicked(message) => {
                    tracing::error!(
                        agent = agent_name,
                        panic = %message,
                        "Background agent panicked"
                    );
                    self.on_failure(
                        agent_name,
                        &shutdown,
                        InternalError::new(format!(
                            "Background agent {agent_name} panicked: {message}"
                        )),
                    )?;
                    BackgroundAgentRestartReason::Panicked
                }
            };

            // Ended during a shutdown: nothing to restart
            if shutdown.is_cancelled() {
                return Ok(());
            }

            consecutive_restarts = if self.was_healthy_since(started_at) {
                1
            } else {
                consecutive_restarts + 1
            };
            let delay = self.restart_backoff.delay(consecutive_restarts);
            self.metrics.on_restart(agent_name, reason);
            tracing::info!(
                agent = agent_name,
                consecutive_restarts,
                ?delay,
                "Restarting background agent after a delay"
            );

            let backoff = self
                .time_source
                .sleep(TimeDelta::from_std(delay).unwrap_or(TimeDelta::MAX));
            if shutdown.run_until_cancelled(backoff).await.is_none() {
                return Ok(());
            }
        }
    }

    /// One run, in a task of its own so that a panic ends the run and not the
    /// supervisor. An abort waits for the aborted task to end, so that nothing
    /// it holds outlives the run.
    async fn run_once(
        &self,
        shutdown: CancellationToken,
        abort: &CancellationToken,
    ) -> AgentRunEnd {
        let agent = self.agent.clone();
        let mut running = JoinSet::new();
        running.spawn(async move { agent.run(shutdown).await });

        let ended = tokio::select! {
            biased;

            ended = running.join_next() => ended,
            () = abort.cancelled() => {
                running.abort_all();
                running.join_next().await;
                return AgentRunEnd::Aborted;
            }
        };

        match ended {
            Some(Ok(Ok(()))) => AgentRunEnd::Returned,
            Some(Ok(Err(e))) => AgentRunEnd::Failed(e),
            Some(Err(join_error)) => AgentRunEnd::Panicked(join_error_message(join_error)),
            None => unreachable!("the set holds the agent's task"),
        }
    }

    /// Records a failure; a fatal one cancels the shutdown token and is
    /// returned
    fn on_failure(
        &self,
        agent_name: &str,
        shutdown: &CancellationToken,
        error: InternalError,
    ) -> Result<(), InternalError> {
        self.metrics.on_failure(agent_name, self.time_source.now());

        if self.agent.is_failure_fatal() && !shutdown.is_cancelled() {
            tracing::error!(
                agent = agent_name,
                "Failure of the background agent is fatal, stopping all agents"
            );
            shutdown.cancel();
            return Err(error);
        }
        Ok(())
    }

    /// A run that lasted at least the longest backoff delay restarts the count
    fn was_healthy_since(&self, started_at: DateTime<Utc>) -> bool {
        let healthy_after = self
            .restart_backoff
            .max_delay
            .unwrap_or(self.restart_backoff.min_delay);
        let ran_for = self.time_source.now() - started_at;
        ran_for
            .to_std()
            .is_ok_and(|ran_for| ran_for >= healthy_after)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How one run of an agent ended
enum AgentRunEnd {
    Returned,
    Aborted,
    Failed(InternalError),
    Panicked(String),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
