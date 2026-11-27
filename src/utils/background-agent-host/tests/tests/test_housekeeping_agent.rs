// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use background_agent_host::{
    BackgroundAgent,
    CancellationToken,
    HousekeepingAgent,
    HousekeepingConfig,
    HousekeepingJobConfig,
    HousekeepingMetrics,
};
use chrono::{DateTime, TimeDelta, Utc};
use internal_error::InternalError;
use pretty_assertions::assert_eq;
use tokio::task::JoinHandle;

use crate::tests::utils::{FakeClock, HostHarness, ScriptedBatch, ScriptedHousekeepingJob};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The agent runs on the fake clock: its pauses and sleeps pass only when a test
// moves it

const JOB: &str = "test.Job";
const OTHER_JOB: &str = "test.OtherJob";

#[test_log::test(tokio::test)]
async fn test_pass_drains_the_backlog_in_bounded_batches() {
    let harness = AgentHarness::new(ScriptedHousekeepingJob::new(
        JOB,
        [
            ScriptedBatch::Removes {
                removed: 3,
                backlog: 4,
            },
            ScriptedBatch::Removes {
                removed: 3,
                backlog: 1,
            },
            ScriptedBatch::Removes {
                removed: 1,
                backlog: 0,
            },
        ],
    ));
    let agent = harness.start();

    harness.job.wait_for_batches(1).await;
    harness.settle().await;
    assert_eq!(harness.job.batch_count(), 1, "waits out the pause");
    assert_eq!(harness.backlog(), 4);
    assert!(!harness.has_finished_a_pass(), "the pass is not over");

    harness.advance(PAUSE).await;
    harness.job.wait_for_batches(2).await;
    harness.advance(PAUSE).await;
    harness.job.wait_for_batches(3).await;
    harness.settle().await;

    assert_eq!(
        harness.job.batches(),
        [
            (harness.start_time, BATCH_SIZE),
            (harness.start_time + PAUSE, BATCH_SIZE),
            (harness.start_time + PAUSE + PAUSE, BATCH_SIZE),
        ]
    );
    assert_eq!(harness.removed(), 7);
    assert_eq!(harness.backlog(), 0);
    assert!(harness.finished_a_pass_now());

    agent.stop(&harness).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_agent_sleeps_until_the_next_deadline_at_most_the_interval() {
    let harness = AgentHarness::new(ScriptedHousekeepingJob::new(JOB, []));
    harness
        .job
        .set_next_due(Some(harness.start_time + TimeDelta::minutes(2)));
    let agent = harness.start();
    harness.job.wait_for_batches(1).await;

    harness.advance(TimeDelta::minutes(1)).await;
    assert_eq!(harness.job.batch_count(), 1);
    harness.job.set_next_due(None);
    harness.advance(TimeDelta::minutes(1)).await;
    harness.job.wait_for_batches(2).await;

    // Without a deadline, once per interval
    harness.advance(INTERVAL - TimeDelta::minutes(1)).await;
    assert_eq!(harness.job.batch_count(), 2);
    harness.advance(TimeDelta::minutes(1)).await;
    harness.job.wait_for_batches(3).await;

    // A deadline beyond the interval waits the interval only
    harness
        .job
        .set_next_due(Some(harness.clock.now() + INTERVAL * 3));
    harness.settle().await;
    harness.advance(INTERVAL).await;
    harness.job.wait_for_batches(4).await;

    agent.stop(&harness).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_shutdown_stops_the_agent_between_batches() {
    let harness = AgentHarness::new(ScriptedHousekeepingJob::new(
        JOB,
        [ScriptedBatch::Removes {
            removed: 3,
            backlog: 10,
        }],
    ));
    let agent = harness.start();
    harness.job.wait_for_batches(1).await;

    agent.stop(&harness).await.unwrap();

    assert_eq!(harness.job.batch_count(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failed_batch_ends_the_run_with_its_error() {
    let harness = AgentHarness::new(ScriptedHousekeepingJob::new(JOB, [ScriptedBatch::Fails]));
    let agent = harness.start();

    assert_matches!(agent.task.await.unwrap(), Err(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_host_runs_each_enabled_job_as_an_agent_of_its_own() {
    let failing = Arc::new(ScriptedHousekeepingJob::new(JOB, [ScriptedBatch::Fails]));
    let other = Arc::new(ScriptedHousekeepingJob::new(OTHER_JOB, []));
    let disabled = Arc::new(ScriptedHousekeepingJob::new("test.DisabledJob", []));
    let host_harness = HostHarness::with_housekeeping_jobs(
        [failing.clone(), other.clone(), disabled.clone()],
        HousekeepingConfig {
            jobs: BTreeMap::from([(
                "test.DisabledJob".to_string(),
                HousekeepingJobConfig {
                    enabled: false,
                    ..AgentHarness::job_config()
                },
            )]),
        },
    );
    let mut host = host_harness.start();

    failing.wait_for_batches(1).await;
    other.wait_for_batches(1).await;
    // The host restarts the failed job, which then finds nothing to do
    host_harness.advance(TimeDelta::seconds(1)).await;
    failing.wait_for_batches(2).await;
    assert_eq!(other.batch_count(), 1, "the other job sleeps on");
    assert_eq!(disabled.batch_count(), 0);

    host.shutdown(&host_harness).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BATCH_SIZE: usize = 3;
const PAUSE: TimeDelta = TimeDelta::seconds(1);
const INTERVAL: TimeDelta = TimeDelta::minutes(5);

struct AgentHarness {
    job: Arc<ScriptedHousekeepingJob>,
    agent: Arc<HousekeepingAgent>,
    metrics: Arc<HousekeepingMetrics>,
    clock: FakeClock,
    start_time: DateTime<Utc>,
    shutdown: CancellationToken,
}

impl AgentHarness {
    fn new(job: ScriptedHousekeepingJob) -> Self {
        let job = Arc::new(job);
        let clock = FakeClock::new();
        let metrics = Arc::new(HousekeepingMetrics::new());
        let agent = Arc::new(HousekeepingAgent::new(
            job.clone(),
            Self::job_config(),
            metrics.clone(),
            Arc::new(clock.time_source().clone()),
        ));

        Self {
            job,
            agent,
            metrics,
            start_time: clock.now(),
            clock,
            shutdown: CancellationToken::new(),
        }
    }

    fn job_config() -> HousekeepingJobConfig {
        HousekeepingJobConfig {
            enabled: true,
            batch_size: BATCH_SIZE,
            interval: INTERVAL.to_std().unwrap(),
            pause_between_batches: PAUSE.to_std().unwrap(),
        }
    }

    fn start(&self) -> RunningAgent {
        let agent = self.agent.clone();
        let shutdown = self.shutdown.clone();
        RunningAgent {
            task: tokio::spawn(async move { agent.run(shutdown).await }),
        }
    }

    async fn settle(&self) {
        self.clock.settle().await;
    }

    async fn advance(&self, delta: TimeDelta) {
        self.clock.advance(delta).await;
    }

    fn removed(&self) -> u64 {
        self.metrics
            .items_removed_total
            .with_label_values(&[JOB, ScriptedHousekeepingJob::KIND])
            .get()
    }

    fn backlog(&self) -> i64 {
        self.metrics.backlog.with_label_values(&[JOB]).get()
    }

    fn last_pass(&self) -> f64 {
        self.metrics
            .last_pass_timestamp_seconds
            .with_label_values(&[JOB])
            .get()
    }

    fn has_finished_a_pass(&self) -> bool {
        !FakeClock::is_unset(self.last_pass())
    }

    /// The last pass finished at the clock's current time
    fn finished_a_pass_now(&self) -> bool {
        self.clock.is_now(self.last_pass())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RunningAgent {
    task: JoinHandle<Result<(), InternalError>>,
}

impl RunningAgent {
    async fn stop(self, harness: &AgentHarness) -> Result<(), InternalError> {
        harness.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), self.task)
            .await
            .expect("the agent stops on a shutdown")
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
