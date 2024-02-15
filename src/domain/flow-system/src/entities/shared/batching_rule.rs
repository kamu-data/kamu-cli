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
    pub min_records_awaited: u64,
    pub max_records_taken: Option<u64>,
    pub max_batching_interval: Option<Duration>,
}

impl BatchingRule {
    pub fn evaluate<'a, I: Iterator<Item = &'a FlowTrigger>>(
        &self,
        awaited_by_now: Duration,
        triggers_it: I,
    ) -> BatchingRuleEvaluation {
        // TODO: it's likely assumed the accumulation is per each input separately
        let mut accumulated_records_count = 0;

        for trigger in triggers_it {
            if let FlowTrigger::InputDatasetFlow(trigger) = trigger {
                match &trigger.flow_result {
                    FlowResult::Empty => {}
                    FlowResult::DatasetUpdate(update) => {
                        accumulated_records_count += update.num_records;
                    }
                }
            }
        }

        assert!(self.min_records_awaited > 0);
        if accumulated_records_count > 0
            && (accumulated_records_count >= self.min_records_awaited
                || self
                    .max_batching_interval
                    .is_some_and(|i| i <= awaited_by_now))
        {
            return BatchingRuleEvaluation {
                accumulated_records_count,
                awaited_by_now,
                satisfied: true,
            };
        }

        // TODO: not very clear how to use `max_records_taken`, especially if bigger
        // block comes, that exceeds this value

        BatchingRuleEvaluation {
            accumulated_records_count,
            awaited_by_now,
            satisfied: false,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct BatchingRuleEvaluation {
    pub accumulated_records_count: u64,
    pub awaited_by_now: Duration,
    pub satisfied: bool,
}

/////////////////////////////////////////////////////////////////////////////////////////
