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

use chrono::TimeDelta;
use internal_error::InternalError;
use observability::tracing::root_span;
use time_source::SystemTimeSource;
use tracing::Instrument;

use crate::{
    BackgroundAgent,
    CancellationToken,
    HousekeepingBatchReport,
    HousekeepingConfig,
    HousekeepingJob,
    HousekeepingJobConfig,
    HousekeepingMetrics,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs one [`HousekeepingJob`]: a pass of batches while items remain due,
/// then a sleep until the job's next deadline, never longer than its interval.
/// The host builds one per enabled job.
pub struct HousekeepingAgent {
    job: Arc<dyn HousekeepingJob>,
    config: HousekeepingJobConfig,
    metrics: Arc<HousekeepingMetrics>,
    time_source: Arc<dyn SystemTimeSource>,
}

impl HousekeepingAgent {
    pub fn new(
        job: Arc<dyn HousekeepingJob>,
        config: HousekeepingJobConfig,
        metrics: Arc<HousekeepingMetrics>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        metrics.init(job.job_name());
        Self {
            job,
            config,
            metrics,
            time_source,
        }
    }

    /// One agent per job of `jobs` that `config` enables
    pub(crate) fn for_enabled_jobs(
        jobs: impl IntoIterator<Item = Arc<dyn HousekeepingJob>>,
        config: &HousekeepingConfig,
        metrics: &Arc<HousekeepingMetrics>,
        time_source: &Arc<dyn SystemTimeSource>,
    ) -> impl Iterator<Item = Self> {
        jobs.into_iter().filter_map(|job| {
            let job_config = config.job(job.job_name());
            job_config
                .enabled
                .then(|| Self::new(job, job_config, metrics.clone(), time_source.clone()))
        })
    }

    /// Runs one batch of the job at the current time
    pub(crate) async fn run_batch(&self) -> Result<HousekeepingBatchReport, InternalError> {
        let job_name = self.job.job_name();
        self.run_batch_impl(job_name)
            .instrument(root_span!("HousekeepingAgent::run_batch", job = job_name,))
            .await
    }

    async fn run_batch_impl(
        &self,
        job_name: &'static str,
    ) -> Result<HousekeepingBatchReport, InternalError> {
        let report = self
            .job
            .run_batch(self.time_source.now(), self.config.batch_size)
            .await?;
        self.metrics.on_batch(job_name, &report);

        let removed = report.removed();
        if removed > 0 {
            tracing::info!(
                job = job_name,
                removed,
                removed_by_kind = ?report.removed_by_kind,
                more_due = report.more_due,
                "Housekeeping batch done"
            );
        }
        Ok(report)
    }

    /// Runs batches until none leaves items due, or `shutdown` stops it
    /// between two batches
    async fn run_pass(&self, shutdown: &CancellationToken) -> Result<PassEnd, InternalError> {
        let job_name = self.job.job_name();
        // Counted after the first batch that leaves items due, then lowered by
        // what each later batch removes: a count may scan the whole backlog
        let mut backlog: Option<u64> = None;
        loop {
            if shutdown.is_cancelled() {
                return Ok(PassEnd::Stopped);
            }
            let report = self.run_batch().await?;
            if !report.more_due {
                self.metrics.on_backlog(job_name, 0);
                self.metrics.on_pass(job_name, self.time_source.now());
                return Ok(PassEnd::Finished);
            }

            let remaining = match backlog {
                None => self.count_due().await?,
                Some(previous) => previous.saturating_sub(report.removed()),
            };
            // The batch left items due, even where the estimate runs out
            let remaining = remaining.max(1);
            backlog = Some(remaining);
            self.metrics.on_backlog(job_name, remaining);

            shutdown
                .run_until_cancelled(
                    self.time_source
                        .sleep(to_time_delta(self.config.pause_between_batches)),
                )
                .await;
        }
    }

    async fn count_due(&self) -> Result<u64, InternalError> {
        self.job
            .count_due(self.time_source.now())
            .instrument(root_span!(
                "HousekeepingAgent::count_due",
                job = self.job.job_name(),
            ))
            .await
    }

    /// How long to sleep after a pass: until the job's next deadline, at most
    /// the interval
    async fn sleep_duration(&self) -> Result<TimeDelta, InternalError> {
        let now = self.time_source.now();
        let interval = to_time_delta(self.config.interval);
        let until_due = self
            .job
            .next_due(now)
            .await?
            .map_or(interval, |next_due| next_due - now);
        Ok(until_due.clamp(TimeDelta::zero(), interval))
    }
}

#[async_trait::async_trait]
impl BackgroundAgent for HousekeepingAgent {
    fn agent_name(&self) -> &'static str {
        self.job.job_name()
    }

    async fn run(&self, shutdown: CancellationToken) -> Result<(), InternalError> {
        loop {
            if self.run_pass(&shutdown).await? == PassEnd::Stopped {
                return Ok(());
            }

            let sleep_duration = self.sleep_duration().await?;
            if shutdown
                .run_until_cancelled(self.time_source.sleep(sleep_duration))
                .await
                .is_none()
            {
                return Ok(());
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassEnd {
    Finished,
    Stopped,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn to_time_delta(duration: Duration) -> TimeDelta {
    TimeDelta::from_std(duration).unwrap_or(TimeDelta::MAX)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
