// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::scalars::FlowID;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct Flow {
    pub flow_id: FlowID,
    pub status: FlowStatus,
    pub outcome: Option<FlowOutcome>,
}

impl From<kamu_flow_system::FlowState> for Flow {
    fn from(value: kamu_flow_system::FlowState) -> Self {
        Self {
            flow_id: value.flow_id.into(),
            status: value.status().into(),
            outcome: value.outcome.map(Into::into),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowStatus")]
pub enum FlowStatus {
    Draft,
    Queued,
    Scheduled,
    Finished,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowOutcome")]
pub enum FlowOutcome {
    Success,
    Failed,
    Cancelled,
    Aborted,
}

/////////////////////////////////////////////////////////////////////////////////////////
