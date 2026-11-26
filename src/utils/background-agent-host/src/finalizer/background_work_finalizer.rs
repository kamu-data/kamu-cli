// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use time_source::SystemTimeSource;

use crate::{
    BackgroundWorkDrain,
    BackgroundWorkDrainOutcome,
    HousekeepingAgent,
    HousekeepingConfig,
    HousekeepingJob,
    HousekeepingMetrics,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Completes the background work a command, a mutating request of the e2e API
/// server, or a test write left behind, for a caller that does not run the
/// agents, or does not wait for them.
///
/// 1. **Settle.** Drains every registered [`BackgroundWorkDrain`] in turn, until one full pass
///    finds all of them idle: the Outbox delivers messages that make resources actionable, and
///    processing them posts messages of its own. A failure is returned, as the caller's effects are
///    then incomplete.
/// 2. **Housekeeping**, when enabled: one batch of each enabled [`HousekeepingJob`], then settle
///    again for the messages the batches posted. A failing job is logged and skipped: the next
///    finalization retries it.
///
/// The drains work on the catalog they are registered in, so the catalog
/// passed to `finalize` is the one that has them.
pub struct BackgroundWorkFinalizer {
    runs_housekeeping: bool,
}

impl BackgroundWorkFinalizer {
    /// Settles only, for callers whose housekeeping runs elsewhere, such as
    /// the agents of an API server
    pub fn settle_only() -> Self {
        Self {
            runs_housekeeping: false,
        }
    }

    /// Settles, then runs one batch of each housekeeping job, for a process
    /// without agents, such as a CLI command
    pub fn with_housekeeping() -> Self {
        Self {
            runs_housekeeping: true,
        }
    }

    pub async fn finalize(&self, catalog: &dill::Catalog) -> Result<(), InternalError> {
        Self::settle(catalog).await?;

        if self.runs_housekeeping && Self::run_housekeeping_batches(catalog).await? > 0 {
            Self::settle(catalog).await?;
        }
        Ok(())
    }

    async fn settle(catalog: &dill::Catalog) -> Result<(), InternalError> {
        let drains = catalog
            .get::<dill::AllOf<dyn BackgroundWorkDrain>>()
            .int_err()?;
        let mut sessions = drains
            .iter()
            .map(|drain| drain.new_drain_session())
            .collect::<Result<Vec<_>, _>>()?;

        loop {
            let mut is_idle = true;
            for session in &mut sessions {
                match session.drain().await? {
                    BackgroundWorkDrainOutcome::Idle => {}
                    BackgroundWorkDrainOutcome::Worked => is_idle = false,
                }
            }
            if is_idle {
                return Ok(());
            }
        }
    }

    /// Returns how many items the batches removed
    async fn run_housekeeping_batches(catalog: &dill::Catalog) -> Result<u64, InternalError> {
        let jobs = catalog
            .get::<dill::AllOf<dyn HousekeepingJob>>()
            .int_err()?;
        if jobs.is_empty() {
            return Ok(0);
        }

        let config = catalog.get_one::<HousekeepingConfig>().int_err()?;
        let metrics = catalog.get_one::<HousekeepingMetrics>().int_err()?;
        let time_source = catalog.get_one::<dyn SystemTimeSource>().int_err()?;

        let mut removed = 0;
        for job in jobs {
            let job_config = config.job(job.job_name());
            if !job_config.enabled {
                continue;
            }

            let job_name = job.job_name();
            let agent =
                HousekeepingAgent::new(job, job_config, metrics.clone(), time_source.clone());
            match agent.run_batch().await {
                Ok(report) => removed += report.removed(),
                Err(e) => tracing::warn!(
                    job = job_name,
                    error = ?e,
                    error_msg = %e,
                    "Housekeeping batch failed, the next one retries it"
                ),
            }
        }
        Ok(removed)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
