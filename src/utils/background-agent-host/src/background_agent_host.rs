// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::TimeDelta;
use dill::component;
use internal_error::{InternalError, ResultIntoInternal};
use time_source::SystemTimeSource;
use tokio::task::JoinSet;

use crate::{
    BackgroundAgent,
    BackgroundAgentHostConfig,
    BackgroundAgentHostMetrics,
    BackgroundAgentSupervisor,
    CancellationToken,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs every registered `BackgroundAgent` until a shutdown.
///
/// Each agent is supervised on its own: a failure, a panic or an early return
/// restarts it after a backoff without affecting the others. A fatal failure
/// shuts every agent down and is returned. On a shutdown, agents get
/// `shutdown_timeout` to finish their current step; the ones still running
/// then are aborted, and `run` returns once their tasks have ended.
pub struct BackgroundAgentHost {
    agents: Vec<Arc<dyn BackgroundAgent>>,
    config: Arc<BackgroundAgentHostConfig>,
    metrics: Arc<BackgroundAgentHostMetrics>,
    time_source: Arc<dyn SystemTimeSource>,
}

#[component(pub)]
impl BackgroundAgentHost {
    pub fn new(
        agents: Vec<Arc<dyn BackgroundAgent>>,
        config: Arc<BackgroundAgentHostConfig>,
        metrics: Arc<BackgroundAgentHostMetrics>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            agents,
            config,
            metrics,
            time_source,
        }
    }

    /// Runs the agents until `shutdown` is cancelled, by the caller or by a
    /// fatal failure, which is then returned.
    ///
    /// # Panics
    /// If no agent is registered: the catalog is misconfigured
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), InternalError> {
        assert!(
            !self.agents.is_empty(),
            "No background agents found! This indicates a DI container configuration issue. Make \
             sure all agent implementations are registered as both their specific trait (e.g., \
             TaskAgent) AND as BackgroundAgent in the DI container."
        );

        self.metrics
            .init(self.agents.iter().map(|agent| agent.agent_name()));

        // Cancelled after the shutdown bound, for the agents still running
        let abort = CancellationToken::new();
        let mut supervisors = JoinSet::new();
        let mut names_by_task = HashMap::new();
        for agent in &self.agents {
            let agent_name = agent.agent_name();
            let supervisor = BackgroundAgentSupervisor::new(
                agent.clone(),
                self.config.restart_backoff,
                self.time_source.clone(),
                self.metrics.clone(),
            );
            let task = supervisors.spawn(supervisor.run(shutdown.clone(), abort.clone()));
            names_by_task.insert(task.id(), agent_name);
        }

        shutdown.cancelled().await;
        tracing::info!(
            timeout = ?self.config.shutdown_timeout,
            "Shutting down background agents"
        );

        let timeout = self
            .time_source
            .sleep(TimeDelta::from_std(self.config.shutdown_timeout).unwrap_or(TimeDelta::MAX));
        tokio::pin!(timeout);

        // Every supervisor is awaited, aborted or not, so that no error is
        // lost and no agent task outlives the host
        let mut outcome = Ok(());
        loop {
            tokio::select! {
                biased;

                ended = supervisors.join_next_with_id() => {
                    let Some(ended) = ended else {
                        break;
                    };
                    let (task_id, result) = match ended {
                        Ok((task_id, result)) => (task_id, result),
                        Err(join_error) => (join_error.id(), Err(join_error).int_err()),
                    };
                    names_by_task.remove(&task_id);
                    if let Err(e) = result
                        && outcome.is_ok()
                    {
                        outcome = Err(e);
                    }
                }
                () = &mut timeout, if !abort.is_cancelled() => {
                    let mut still_running: Vec<_> = names_by_task.values().copied().collect();
                    still_running.sort_unstable();
                    tracing::warn!(
                        agents = ?still_running,
                        "Background agents did not stop in time, aborting them"
                    );
                    abort.cancel();
                }
            }
        }

        tracing::info!("Background agents stopped");
        outcome
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
