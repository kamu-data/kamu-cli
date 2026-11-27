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

use background_agent_host::{
    BackgroundAgent,
    BackgroundAgentHost,
    BackgroundAgentHostConfig,
    BackgroundAgentHostMetrics,
    BackgroundAgentRestartReason,
    CancellationToken,
    HousekeepingConfig,
    HousekeepingJob,
    HousekeepingMetrics,
};
use chrono::TimeDelta;
use internal_error::InternalError;
use retry_backoff::{RetryBackoff, RetryBackoffType};
use tokio::task::JoinHandle;

use super::{FakeClock, ScriptedHousekeepingJob};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A [`BackgroundAgentHost`] on a [`FakeClock`]: restart backoffs and the
/// shutdown bound pass only when a test moves it
pub struct HostHarness {
    pub host: Arc<BackgroundAgentHost>,
    clock: FakeClock,
    metrics: Arc<BackgroundAgentHostMetrics>,
    shutdown: CancellationToken,
}

impl HostHarness {
    pub const MAX_RESTART_DELAY: TimeDelta = TimeDelta::seconds(4);
    pub const SHUTDOWN_TIMEOUT: TimeDelta = TimeDelta::seconds(10);

    pub fn new<A: BackgroundAgent + 'static, const N: usize>(agents: [Arc<A>; N]) -> Self {
        Self::with_shutdown_timeout(agents, Self::SHUTDOWN_TIMEOUT)
    }

    pub fn with_shutdown_timeout<A: BackgroundAgent + 'static, const N: usize>(
        agents: [Arc<A>; N],
        shutdown_timeout: TimeDelta,
    ) -> Self {
        Self::build(
            agents
                .into_iter()
                .map(|agent| agent as Arc<dyn BackgroundAgent>)
                .collect(),
            Vec::new(),
            HousekeepingConfig::default(),
            shutdown_timeout,
        )
    }

    pub fn without_agents() -> Self {
        Self::build(
            Vec::new(),
            Vec::new(),
            HousekeepingConfig::default(),
            Self::SHUTDOWN_TIMEOUT,
        )
    }

    /// Hosts no agents of its own, only the housekeeping jobs
    pub fn with_housekeeping_jobs<const N: usize>(
        jobs: [Arc<ScriptedHousekeepingJob>; N],
        housekeeping_config: HousekeepingConfig,
    ) -> Self {
        Self::build(
            Vec::new(),
            jobs.into_iter()
                .map(|job| job as Arc<dyn HousekeepingJob>)
                .collect(),
            housekeeping_config,
            Self::SHUTDOWN_TIMEOUT,
        )
    }

    fn build(
        agents: Vec<Arc<dyn BackgroundAgent>>,
        housekeeping_jobs: Vec<Arc<dyn HousekeepingJob>>,
        housekeeping_config: HousekeepingConfig,
        shutdown_timeout: TimeDelta,
    ) -> Self {
        let clock = FakeClock::new();
        let metrics = Arc::new(BackgroundAgentHostMetrics::new());
        let config = BackgroundAgentHostConfig {
            restart_backoff: RetryBackoff {
                backoff_type: RetryBackoffType::Exponential,
                min_delay: Duration::from_secs(1),
                max_delay: Some(Self::MAX_RESTART_DELAY.to_std().unwrap()),
            },
            shutdown_timeout: shutdown_timeout.to_std().unwrap(),
        };

        let host = Arc::new(BackgroundAgentHost::new(
            agents,
            housekeeping_jobs,
            Arc::new(housekeeping_config),
            Arc::new(HousekeepingMetrics::new()),
            Arc::new(config),
            metrics.clone(),
            Arc::new(clock.time_source().clone()),
        ));

        Self {
            host,
            clock,
            metrics,
            shutdown: CancellationToken::new(),
        }
    }

    pub fn start(&self) -> RunningHost {
        let host = self.host.clone();
        let shutdown = self.shutdown.clone();
        RunningHost {
            task: tokio::spawn(async move { host.run(shutdown).await }),
        }
    }

    pub fn request_shutdown(&self) {
        self.shutdown.cancel();
    }

    pub async fn advance(&self, delta: TimeDelta) {
        self.clock.advance(delta).await;
    }

    pub async fn settle(&self) {
        self.clock.settle().await;
    }

    /// The last failure time is the clock's current time
    pub fn last_failed_now(&self, agent: &str) -> bool {
        self.clock.is_now(self.last_failure(agent))
    }

    pub fn never_failed(&self, agent: &str) -> bool {
        FakeClock::is_unset(self.last_failure(agent))
    }

    pub fn restarts(&self, agent: &str, reason: BackgroundAgentRestartReason) -> u64 {
        self.metrics
            .restarts_total
            .with_label_values(&[agent, reason.into()])
            .get()
    }

    pub fn running(&self, agent: &str) -> i64 {
        self.metrics.running.with_label_values(&[agent]).get()
    }

    fn last_failure(&self, agent: &str) -> f64 {
        self.metrics
            .last_failure_timestamp_seconds
            .with_label_values(&[agent])
            .get()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RunningHost {
    task: JoinHandle<Result<(), InternalError>>,
}

impl RunningHost {
    pub fn has_ended(&self) -> bool {
        self.task.is_finished()
    }

    /// Waits for the host to return. Real time, bounded, so that a host that
    /// never ends fails the test
    pub async fn wait_for_end(&mut self) -> Result<(), InternalError> {
        tokio::time::timeout(Duration::from_secs(5), &mut self.task)
            .await
            .expect("the host did not end")
            .unwrap()
    }

    /// Requests a shutdown and waits for the host to return
    pub async fn shutdown(&mut self, harness: &HostHarness) -> Result<(), InternalError> {
        harness.request_shutdown();
        self.wait_for_end().await
    }
}

impl Drop for RunningHost {
    fn drop(&mut self) {
        self.task.abort();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
