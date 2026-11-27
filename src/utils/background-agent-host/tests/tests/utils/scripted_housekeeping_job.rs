// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use background_agent_host::{HousekeepingBatchReport, HousekeepingJob};
use chrono::{DateTime, Utc};
use internal_error::InternalError;

use super::EventCounter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One batch of a [`ScriptedHousekeepingJob`]
pub enum ScriptedBatch {
    /// Removes `removed` items, leaving `backlog`
    Removes {
        removed: u64,
        backlog: u64,
    },
    Fails,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs its script, one entry per batch, then batches that find nothing due.
/// Its deadline is whatever [`Self::set_next_due`] last set.
pub struct ScriptedHousekeepingJob {
    name: &'static str,
    script: Mutex<VecDeque<ScriptedBatch>>,
    next_due: Mutex<Option<DateTime<Utc>>>,
    batches: Mutex<Vec<(DateTime<Utc>, usize)>>,
    batch_count: EventCounter,
}

impl ScriptedHousekeepingJob {
    pub const KIND: &'static str = "TestItem";

    pub fn new<const N: usize>(name: &'static str, script: [ScriptedBatch; N]) -> Self {
        Self {
            name,
            script: Mutex::new(script.into()),
            next_due: Mutex::new(None),
            batches: Mutex::new(Vec::new()),
            batch_count: EventCounter::new(),
        }
    }

    pub fn set_next_due(&self, next_due: Option<DateTime<Utc>>) {
        *self.next_due.lock().unwrap() = next_due;
    }

    pub fn batch_count(&self) -> usize {
        self.batch_count.get()
    }

    /// The time and size of each batch run so far
    pub fn batches(&self) -> Vec<(DateTime<Utc>, usize)> {
        self.batches.lock().unwrap().clone()
    }

    pub async fn wait_for_batches(&self, total: usize) {
        self.batch_count
            .wait_for(total, format!("batches of {}", self.name))
            .await;
    }
}

#[async_trait::async_trait]
impl HousekeepingJob for ScriptedHousekeepingJob {
    fn job_name(&self) -> &'static str {
        self.name
    }

    async fn run_batch(
        &self,
        now: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<HousekeepingBatchReport, InternalError> {
        let batch = self.script.lock().unwrap().pop_front();
        self.batches.lock().unwrap().push((now, batch_size));
        self.batch_count.increment();

        match batch {
            None => Ok(HousekeepingBatchReport::default()),
            Some(ScriptedBatch::Removes { removed, backlog }) => Ok(HousekeepingBatchReport {
                removed_by_kind: BTreeMap::from([(Self::KIND.to_string(), removed)]),
                backlog,
            }),
            Some(ScriptedBatch::Fails) => Err(InternalError::new("scripted batch failure")),
        }
    }

    async fn next_due(&self, _now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, InternalError> {
        Ok(*self.next_due.lock().unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
