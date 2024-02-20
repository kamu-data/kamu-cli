// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;
use thiserror::Error;

use crate::{FlowResult, FlowTrigger};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchingRule {
    min_records_to_await: u64,
    max_batching_interval: Duration,
}

impl BatchingRule {
    const MAX_BATCHING_INTERVAL_HOURS: i64 = 24;

    pub fn new_checked(
        min_records_to_await: u64,
        max_batching_interval: Duration,
    ) -> Result<Self, BatchingRuleValidationError> {
        if min_records_to_await == 0 {
            return Err(BatchingRuleValidationError::MinRecordsToAwaitNotPositive);
        }

        let max_possible_interval = Duration::hours(Self::MAX_BATCHING_INTERVAL_HOURS);
        if max_batching_interval > max_possible_interval {
            return Err(BatchingRuleValidationError::MaxIntervalAboveLimit);
        }

        Ok(Self {
            min_records_to_await,
            max_batching_interval,
        })
    }

    #[inline]
    pub fn min_records_to_await(&self) -> u64 {
        self.min_records_to_await
    }

    #[inline]
    pub fn max_batching_interval(&self) -> &Duration {
        &self.max_batching_interval
    }

    pub fn evaluate(
        &self,
        awaited_by_now: Duration,
        triggers: &[FlowTrigger],
    ) -> BatchingRuleEvaluation {
        // TODO: it's likely assumed the accumulation is per each input separately, but
        // for now count overall number
        let mut accumulated_records_count = 0;
        let mut watermark_modified = false;

        // Scan each accumulated trigger to decide
        for trigger in triggers {
            if let FlowTrigger::InputDatasetFlow(trigger) = trigger {
                match &trigger.flow_result {
                    FlowResult::Empty => {}
                    FlowResult::DatasetUpdate(update) => {
                        accumulated_records_count += update.num_records;
                        watermark_modified |= update.watermark_modified;
                    }
                }
            }
        }

        // The conditoin is satisfied if
        //   - we crossed the number of new records threshold
        //   - or waited long enough, assuming
        //      - there is at least some change of the inputs
        //      - watmermark got touched
        let satisfied = (accumulated_records_count > 0 || watermark_modified)
            && (accumulated_records_count >= self.min_records_to_await
                || self.max_batching_interval <= awaited_by_now);

        BatchingRuleEvaluation {
            awaited_by_now,
            accumulated_records_count,
            watermark_modified,
            satisfied,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum BatchingRuleValidationError {
    #[error("Minimum records to await must be a positive number")]
    MinRecordsToAwaitNotPositive,

    #[error(
        "Maximum interval to await should not exceed {} hours",
        BatchingRule::MAX_BATCHING_INTERVAL_HOURS
    )]
    MaxIntervalAboveLimit,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct BatchingRuleEvaluation {
    pub awaited_by_now: Duration,
    pub accumulated_records_count: u64,
    pub watermark_modified: bool,
    pub satisfied: bool,
}

/////////////////////////////////////////////////////////////////////////////////////////
