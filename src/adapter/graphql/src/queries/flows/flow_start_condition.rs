// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
    pub current_records_accumulated: u64,
    pub active_batching_rule: FlowConfigurationBatching,
}

impl From<fs::FlowStartConditionBatching> for FlowStartConditionBatching {
    fn from(value: fs::FlowStartConditionBatching) -> Self {
        Self {
            current_records_accumulated: value.current_records_accumulated,
            active_batching_rule: value.active_batching_rule.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
