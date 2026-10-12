// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Counts of flow processes in a group by effective state
#[derive(SimpleObject, Copy, Clone, PartialEq, Eq)]
pub struct FlowProcessGroupRollup {
    /// Number of processes in the group
    pub total: u32,
    /// Number of processes running without failures
    pub active: u32,
    /// Number of processes with failures below the stop threshold
    pub failing: u32,
    /// Number of processes paused by a user
    pub paused: u32,
    /// Number of processes without a trigger
    pub unconfigured: u32,
    /// Number of processes stopped automatically
    pub stopped: u32,
    /// Longest streak of failed runs in a row among the processes
    pub worst_consecutive_failures: u32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<kamu_flow_system::FlowProcessGroupRollup> for FlowProcessGroupRollup {
    fn from(value: kamu_flow_system::FlowProcessGroupRollup) -> Self {
        Self {
            total: value.total,
            active: value.active,
            failing: value.failing,
            paused: value.paused,
            unconfigured: value.unconfigured,
            stopped: value.stopped,
            worst_consecutive_failures: value.worst_consecutive_failures,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
