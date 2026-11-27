// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
use BackgroundWorkDrainOutcome::{Idle, Worked};
use background_agent_host::{
    BackgroundWorkDrain,
    BackgroundWorkDrainOutcome,
    BackgroundWorkDrainSession,
    BackgroundWorkFinalizer,
    HousekeepingBatchReport,
    HousekeepingConfig,
    HousekeepingJob,
    HousekeepingJobConfig,
    HousekeepingMetrics,
};
use chrono::{DateTime, Utc};
use internal_error::InternalError;
use pretty_assertions::assert_eq;
use time_source::{FakeSystemTimeSource, SystemTimeSource};

use crate::tests::utils::{FakeClock, ScriptedBatch, ScriptedHousekeepingJob};

#[test_log::test(tokio::test)]
async fn test_settle_drains_until_a_full_pass_finds_all_idle() {
    let outbox = Arc::new(ScriptedDrain::new([Worked, Idle, Idle]));
    let controllers = Arc::new(ScriptedDrain::new([Idle, Worked, Idle]));
    let harness = FinalizerHarness::new([outbox.clone(), controllers.clone()], []);

    harness
        .finalize(BackgroundWorkFinalizer::settle_only())
        .await
        .unwrap();

    // The work of the second pass may have made the first drain's work due
    assert_eq!(outbox.drains(), 3);
    assert_eq!(controllers.drains(), 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_settle_only_runs_no_housekeeping() {
    let job = Arc::new(ScriptedHousekeepingJob::new(JOB, []));
    let harness = FinalizerHarness::new([], [job.clone()]);

    harness
        .finalize(BackgroundWorkFinalizer::settle_only())
        .await
        .unwrap();

    assert_eq!(job.batch_count(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_housekeeping_runs_one_batch_per_enabled_job_then_settles_again() {
    let drain = Arc::new(ScriptedDrain::new([]));
    let job = Arc::new(ScriptedHousekeepingJob::new(
        JOB,
        [ScriptedBatch::Removes {
            removed: 2,
            backlog: 5,
        }],
    ));
    let disabled_job = Arc::new(ScriptedHousekeepingJob::new(DISABLED_JOB, []));
    let harness = FinalizerHarness::new([drain.clone()], [job.clone(), disabled_job.clone()]);

    harness
        .finalize(BackgroundWorkFinalizer::with_housekeeping())
        .await
        .unwrap();

    assert_eq!(
        job.batches(),
        [(harness.clock.now(), BATCH_SIZE)],
        "one batch, though a backlog remains"
    );
    assert_eq!(disabled_job.batch_count(), 0);
    assert_eq!(drain.drains(), 2, "settled before and after the batch");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_housekeeping_that_removed_nothing_needs_no_second_settle() {
    let drain = Arc::new(ScriptedDrain::new([]));
    let job = Arc::new(ScriptedHousekeepingJob::new(JOB, []));
    let harness = FinalizerHarness::new([drain.clone()], [job.clone()]);

    harness
        .finalize(BackgroundWorkFinalizer::with_housekeeping())
        .await
        .unwrap();

    assert_eq!(job.batch_count(), 1);
    assert_eq!(drain.drains(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failing_housekeeping_job_does_not_fail_the_finalization() {
    let failing = Arc::new(ScriptedHousekeepingJob::new(JOB, [ScriptedBatch::Fails]));
    let other = Arc::new(ScriptedHousekeepingJob::new(
        OTHER_JOB,
        [ScriptedBatch::Removes {
            removed: 1,
            backlog: 0,
        }],
    ));
    let harness = FinalizerHarness::new([], [failing.clone(), other.clone()]);

    harness
        .finalize(BackgroundWorkFinalizer::with_housekeeping())
        .await
        .unwrap();

    assert_eq!(failing.batch_count(), 1);
    assert_eq!(other.batch_count(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failing_drain_fails_the_finalization() {
    let harness = FinalizerHarness::new([Arc::new(ScriptedDrain::failing())], []);

    let result = harness
        .finalize(BackgroundWorkFinalizer::settle_only())
        .await;

    assert!(result.is_err());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const JOB: &str = "test.Job";
const OTHER_JOB: &str = "test.OtherJob";
const DISABLED_JOB: &str = "test.DisabledJob";
const BATCH_SIZE: usize = 7;

struct FinalizerHarness {
    catalog: dill::Catalog,
    clock: FakeClock,
}

impl FinalizerHarness {
    fn new<const D: usize, const J: usize>(
        drains: [Arc<ScriptedDrain>; D],
        jobs: [Arc<ScriptedHousekeepingJob>; J],
    ) -> Self {
        let clock = FakeClock::new();
        let job_config = HousekeepingJobConfig {
            batch_size: BATCH_SIZE,
            ..HousekeepingJobConfig::default()
        };

        let mut b = dill::CatalogBuilder::new();
        for (i, drain) in drains.into_iter().enumerate() {
            match i {
                0 => Slot::<0, ScriptedDrain>::register_drain(&mut b, drain),
                1 => Slot::<1, ScriptedDrain>::register_drain(&mut b, drain),
                _ => unimplemented!("at most 2 drains"),
            }
        }
        for (i, job) in jobs.into_iter().enumerate() {
            match i {
                0 => Slot::<0, ScriptedHousekeepingJob>::register_job(&mut b, job),
                1 => Slot::<1, ScriptedHousekeepingJob>::register_job(&mut b, job),
                _ => unimplemented!("at most 2 jobs"),
            }
        }
        b.add_value(HousekeepingConfig {
            jobs: BTreeMap::from([
                (JOB.to_string(), job_config.clone()),
                (OTHER_JOB.to_string(), job_config.clone()),
                (
                    DISABLED_JOB.to_string(),
                    HousekeepingJobConfig {
                        enabled: false,
                        ..job_config
                    },
                ),
            ]),
        });
        b.add::<HousekeepingMetrics>();
        b.add_value(clock.time_source().clone())
            .bind::<dyn SystemTimeSource, FakeSystemTimeSource>();

        Self {
            catalog: b.build(),
            clock,
        }
    }

    async fn finalize(&self, finalizer: BackgroundWorkFinalizer) -> Result<(), InternalError> {
        finalizer.finalize(&self.catalog).await
    }
}

/// One of several test doubles of one type in a catalog, which keeps a single
/// registration per concrete type
struct Slot<const I: usize, T: ?Sized>(Arc<T>);

impl<const I: usize> Slot<I, ScriptedDrain> {
    fn register_drain(b: &mut dill::CatalogBuilder, drain: Arc<ScriptedDrain>) {
        b.add_value(Self(drain))
            .bind::<dyn BackgroundWorkDrain, Self>();
    }
}

impl<const I: usize> Slot<I, ScriptedHousekeepingJob> {
    fn register_job(b: &mut dill::CatalogBuilder, job: Arc<ScriptedHousekeepingJob>) {
        b.add_value(Self(job)).bind::<dyn HousekeepingJob, Self>();
    }
}

impl<const I: usize> BackgroundWorkDrain for Slot<I, ScriptedDrain> {
    fn new_drain_session(&self) -> Result<Box<dyn BackgroundWorkDrainSession + '_>, InternalError> {
        self.0.new_drain_session()
    }
}

#[async_trait::async_trait]
impl<const I: usize> HousekeepingJob for Slot<I, ScriptedHousekeepingJob> {
    fn job_name(&self) -> &'static str {
        self.0.job_name()
    }

    async fn run_batch(
        &self,
        now: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<HousekeepingBatchReport, InternalError> {
        self.0.run_batch(now, batch_size).await
    }

    async fn next_due(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, InternalError> {
        self.0.next_due(now).await
    }
}

/// Reports its script, one outcome per drain, then idle drains
struct ScriptedDrain {
    script: Mutex<VecDeque<BackgroundWorkDrainOutcome>>,
    fails: bool,
    drains: AtomicUsize,
}

impl ScriptedDrain {
    fn new<const N: usize>(script: [BackgroundWorkDrainOutcome; N]) -> Self {
        Self {
            script: Mutex::new(script.into()),
            fails: false,
            drains: AtomicUsize::new(0),
        }
    }

    fn failing() -> Self {
        Self {
            fails: true,
            ..Self::new([])
        }
    }

    fn drains(&self) -> usize {
        self.drains.load(Ordering::SeqCst)
    }
}

impl BackgroundWorkDrain for ScriptedDrain {
    fn new_drain_session(&self) -> Result<Box<dyn BackgroundWorkDrainSession + '_>, InternalError> {
        Ok(Box::new(ScriptedDrainSession { drain: self }))
    }
}

struct ScriptedDrainSession<'a> {
    drain: &'a ScriptedDrain,
}

#[async_trait::async_trait]
impl BackgroundWorkDrainSession for ScriptedDrainSession<'_> {
    async fn drain(&mut self) -> Result<BackgroundWorkDrainOutcome, InternalError> {
        self.drain.drains.fetch_add(1, Ordering::SeqCst);
        if self.drain.fails {
            return Err(InternalError::new("scripted drain failure"));
        }
        Ok(self
            .drain
            .script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Idle))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
