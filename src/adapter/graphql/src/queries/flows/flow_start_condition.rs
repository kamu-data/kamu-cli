// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_datasets::DatasetIntervalIncrement;
use kamu_flow_system as fs;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowStartCondition {
    Schedule(FlowStartConditionSchedule),
    Throttling(FlowStartConditionThrottling),
    Batching(FlowStartConditionBatching),
    Executor(FlowStartConditionExecutor),
}

impl FlowStartCondition {
    pub fn create_from_raw_flow_data(
        start_condition: &fs::FlowStartCondition,
        matching_activation_causes: &[fs::FlowActivationCause],
    ) -> Self {
        match start_condition {
            fs::FlowStartCondition::Schedule(s) => Self::Schedule(FlowStartConditionSchedule {
                wake_up_at: s.wake_up_at,
            }),
            fs::FlowStartCondition::Throttling(t) => Self::Throttling((*t).into()),
            fs::FlowStartCondition::Batching(b) => {
                // Start from zero increment
                let mut total_increment = DatasetIntervalIncrement::default();

                // TODO: somehow limit dataset traversal to blocks that existed at the time of
                // flow latest event, as they might have evolved after this state was loaded

                // For each dataset activation cause, add accumulated changes since the initial
                for activation_cause in matching_activation_causes {
                    if let fs::FlowActivationCause::DatasetUpdate(update_cause) = activation_cause
                        && let fs::DatasetChanges::NewData {
                            blocks_added,
                            records_added,
                            new_watermark,
                        } = update_cause.changes
                    {
                        total_increment += DatasetIntervalIncrement {
                            num_blocks: blocks_added,
                            num_records: records_added,
                            updated_watermark: new_watermark,
                        };
                    }
                }

                // Finally, present the full picture from condition + computed view results
                Self::Batching(FlowStartConditionBatching {
                    active_batching_rule: b.active_batching_rule.into(),
                    batching_deadline: b.batching_deadline,
                    accumulated_records_count: total_increment.num_records,
                    watermark_modified: total_increment.updated_watermark.is_some(),
                })
            }
            fs::FlowStartCondition::Executor(e) => Self::Executor(FlowStartConditionExecutor {
                task_id: e.task_id.into(),
            }),
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionSchedule {
    wake_up_at: DateTime<Utc>,
}

#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionThrottling {
    interval_sec: i64,
    wake_up_at: DateTime<Utc>,
    shifted_from: DateTime<Utc>,
}

impl From<fs::FlowStartConditionThrottling> for FlowStartConditionThrottling {
    fn from(value: fs::FlowStartConditionThrottling) -> Self {
        Self {
            interval_sec: value.interval.num_seconds(),
            wake_up_at: value.wake_up_at,
            shifted_from: value.shifted_from,
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionBatching {
    pub active_batching_rule: FlowTriggerBatchingRule,
    pub batching_deadline: DateTime<Utc>,
    pub accumulated_records_count: u64,
    pub watermark_modified: bool,
    // TODO: we can list all applied input flows, if that is interesting for debugging
}

#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionExecutor {
    pub task_id: TaskID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
