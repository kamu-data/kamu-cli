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

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum FlowRunOrder {
    /// Queue-oriented ordering:
    ///   - by status: waiting, running, retrying, finished
    ///   - by most recent activity
    Queue,

    /// Earliest scheduled activation first.
    ScheduledForActivation,
}

impl FlowRunOrder {
    pub fn into_domain(self) -> kamu_flow_system::FlowOrder {
        match self {
            Self::Queue => kamu_flow_system::FlowOrder::queued(),
            Self::ScheduledForActivation => kamu_flow_system::FlowOrder::scheduled_activation(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
