// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;

use crate::{FlowResult, FlowTrigger};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchingRule {
    min_records_awaited: u64,
    max_batching_interval: Option<Duration>,
}

impl BatchingRule {
    pub fn new(min_records_awaited: u64, max_batching_interval: Option<Duration>) -> Self {
        assert!(min_records_awaited > 0);

        Self {
            min_records_awaited,
            max_batching_interval,
        }
    }

    #[inline]
    pub fn min_records_awaited(&self) -> u64 {
        self.min_records_awaited
    }

    #[inline]
    pub fn max_batching_interval(&self) -> &Option<Duration> {
        &self.max_batching_interval
    }

    pub fn evaluate<'a, I: Iterator<Item = &'a FlowTrigger>>(
        &self,
        awaited_by_now: Duration,
        triggers_it: I,
    ) -> BatchingRuleEvaluation {
        // TODO: it's likely assumed the accumulation is per each input separately, but
        // for now count overall number
        let mut accumulated_records_count = 0;
        let mut watermark_modified = false;

        // Scan each accumulated trigger to decide
        for trigger in triggers_it {
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
            && (accumulated_records_count >= self.min_records_awaited
                || self
                    .max_batching_interval
                    .is_some_and(|i| i <= awaited_by_now));

        BatchingRuleEvaluation {
            awaited_by_now,
            accumulated_records_count,
            watermark_modified,
            satisfied,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct BatchingRuleEvaluation {
    pub awaited_by_now: Duration,
    pub accumulated_records_count: u64,
    pub watermark_modified: bool,
    pub satisfied: bool,
}

/////////////////////////////////////////////////////////////////////////////////////////
