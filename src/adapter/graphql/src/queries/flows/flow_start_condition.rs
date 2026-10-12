// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_flow_system::{self as fs};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What a waiting flow waits for before it runs
#[derive(Union)]
pub(crate) enum FlowStartCondition {
    Schedule(FlowStartConditionSchedule),
    Throttling(FlowStartConditionThrottling),
    Reactive(FlowStartConditionReactive),
    Executor(FlowStartConditionExecutor),
}

impl FlowStartCondition {
    pub fn create_from_raw_flow_data(
        flow_state: &fs::FlowState,
        start_condition: &fs::FlowStartCondition,
    ) -> Self {
        match start_condition {
            fs::FlowStartCondition::Schedule(s) => Self::Schedule(FlowStartConditionSchedule {
                wake_up_at: s.wake_up_at,
            }),
            fs::FlowStartCondition::Throttling(t) => Self::Throttling((*t).into()),
            fs::FlowStartCondition::Reactive(b) => {
                let total_increment =
                    flow_state.get_reactive_data_increment(b.last_activation_cause_index);

                // Finally, present the full picture from condition + computed view results
                Self::Reactive(FlowStartConditionReactive {
                    active_batching_rule: b.active_rule.for_new_data.into(),
                    batching_deadline: b.batching_deadline,
                    accumulated_records_count: total_increment.records_added,
                    watermark_modified: total_increment.new_watermark.is_some(),
                    for_breaking_change: b.active_rule.for_breaking_change.into(),
                })
            }
            fs::FlowStartCondition::Executor(e) => Self::Executor(FlowStartConditionExecutor {
                task_id: e.task_id.into(),
            }),
        }
    }
}

/// Flow waits for its scheduled time
#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionSchedule {
    /// Time the flow is scheduled to run
    wake_up_at: DateTime<Utc>,
}

/// Flow was delayed because its previous run was too recent
#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionThrottling {
    /// Minimum interval between runs, in seconds
    interval_sec: i64,
    /// Time the flow will run
    wake_up_at: DateTime<Utc>,
    /// Time the flow would have run without throttling
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

/// Flow waits for enough new input data
#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionReactive {
    /// Batching rule in effect for this flow
    pub active_batching_rule: FlowTriggerBatchingRule,
    /// Time the flow runs even if too few records arrived
    pub batching_deadline: DateTime<Utc>,
    /// Number of new input records accumulated so far
    pub accumulated_records_count: u64,
    /// Whether any input advanced its watermark
    pub watermark_modified: bool,
    /// How to react when an input's history is rewritten
    pub for_breaking_change: FlowTriggerBreakingChangeRule,
    // TODO: we can list all applied input flows, if that is interesting for debugging
}

/// Flow waits for its task to be picked up by an executor
#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionExecutor {
    /// Identifier of the scheduled task
    pub task_id: TaskID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
