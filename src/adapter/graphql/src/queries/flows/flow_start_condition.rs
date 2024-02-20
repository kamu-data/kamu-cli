// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_flow_system as fs;

use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowStartCondition {
    Throttling(FlowStartConditionThrottling),
    Batching(FlowStartConditionBatching),
}

impl From<fs::FlowStartCondition> for FlowStartCondition {
    fn from(value: fs::FlowStartCondition) -> Self {
        match value {
            fs::FlowStartCondition::Throttling(t) => Self::Throttling(t.into()),
            fs::FlowStartCondition::Batching(b) => Self::Batching(b.into()),
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionThrottling {
    interval_sec: i64,
}

impl From<fs::FlowStartConditionThrottling> for FlowStartConditionThrottling {
    fn from(value: fs::FlowStartConditionThrottling) -> Self {
        Self {
            interval_sec: value.interval.num_seconds(),
        }
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowStartConditionBatching {
    pub active_batching_rule: FlowConfigurationBatching,
    pub batching_deadline: DateTime<Utc>,
    pub accumulated_records_count: u64,
    pub watermark_modified: bool,
}

impl From<fs::FlowStartConditionBatching> for FlowStartConditionBatching {
    fn from(value: fs::FlowStartConditionBatching) -> Self {
        Self {
            active_batching_rule: value.active_batching_rule.into(),
            batching_deadline: value.batching_deadline,
            accumulated_records_count: value.accumulated_records_count,
            watermark_modified: value.watermark_modified,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
