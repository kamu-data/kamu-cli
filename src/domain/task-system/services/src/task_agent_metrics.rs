// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use dill::*;
use kamu_task_system::{TaskOutcome, TaskOutcomeKind};
use observability::metrics::{MetricsProvider, seconds_between, unix_timestamp_seconds};
use strum::IntoEnumIterator as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Tasks run from seconds (probes, no-op polls) to hours (large ingests)
const TASK_TIME_BUCKETS_SECONDS: &[f64] = &[
    1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0, 21600.0,
];

/// The only task executor for now: the task agent runs tasks in-process, one
/// at a time. Clustered deployments will run several executors
pub const MAIN_TASK_EXECUTOR: &str = "main";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskAgentMetrics {
    pub task_duration_seconds: prometheus::HistogramVec,
    pub task_queue_wait_seconds: prometheus::Histogram,
    pub running_task_started_timestamp_seconds: prometheus::GaugeVec,
}

#[component(pub)]
#[interface(dyn MetricsProvider)]
#[scope(Singleton)]
impl TaskAgentMetrics {
    pub fn new() -> Self {
        use prometheus::*;

        Self {
            // Split by outcome, as failures and cancellations have timings of their
            // own. Only runs are counted: tasks cancelled while queued, or while
            // running across a restart, never ran here
            task_duration_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "task_agent_task_duration_seconds",
                    "Time a task spent running, from being taken off the queue to its outcome, by \
                     logical plan type and outcome",
                )
                .buckets(TASK_TIME_BUCKETS_SECONDS.to_vec()),
                &["plan_type", "outcome"],
            )
            .unwrap(),
            // Not split by plan type: the queue is FIFO, so a wait depends on the
            // tasks ahead, not on the task's own type
            task_queue_wait_seconds: Histogram::with_opts(
                HistogramOpts::new(
                    "task_agent_task_queue_wait_seconds",
                    "Time a task spent queued before the task agent took it",
                )
                .buckets(TASK_TIME_BUCKETS_SECONDS.to_vec()),
            )
            .unwrap(),
            running_task_started_timestamp_seconds: GaugeVec::new(
                Opts::new(
                    "task_agent_running_task_started_timestamp_seconds",
                    "Time when the task currently running on an executor started, 0 when the \
                     executor is idle",
                ),
                &["executor"],
            )
            .unwrap(),
        }
    }

    /// Initializes labeled metrics so they show up in the output early
    pub(crate) fn init<'a>(&self, plan_types: impl Iterator<Item = &'a str>) {
        for plan_type in plan_types {
            for outcome in TaskOutcomeKind::iter() {
                self.task_duration_seconds
                    .with_label_values(&[plan_type, outcome.into()]);
            }
        }
        self.running_task_started_timestamp_seconds
            .with_label_values(&[MAIN_TASK_EXECUTOR]);
    }

    pub(crate) fn on_task_started(&self, created_at: DateTime<Utc>, started_at: DateTime<Utc>) {
        self.task_queue_wait_seconds
            .observe(seconds_between(created_at, started_at));

        self.running_task_started_timestamp_seconds
            .with_label_values(&[MAIN_TASK_EXECUTOR])
            .set(unix_timestamp_seconds(started_at));
    }

    pub(crate) fn on_task_finished(
        &self,
        plan_type: &str,
        outcome: &TaskOutcome,
        started_at: DateTime<Utc>,
        finished_at: DateTime<Utc>,
    ) {
        self.task_duration_seconds
            .with_label_values(&[plan_type, TaskOutcomeKind::from(outcome).into()])
            .observe(seconds_between(started_at, finished_at));

        self.running_task_started_timestamp_seconds
            .with_label_values(&[MAIN_TASK_EXECUTOR])
            .set(0.0);
    }
}

impl MetricsProvider for TaskAgentMetrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(self.task_duration_seconds.clone()))?;
        reg.register(Box::new(self.task_queue_wait_seconds.clone()))?;
        reg.register(Box::new(
            self.running_task_started_timestamp_seconds.clone(),
        ))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
