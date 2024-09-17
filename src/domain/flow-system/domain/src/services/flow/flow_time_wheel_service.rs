// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::FlowID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait FlowTimeWheelService: Send + Sync {
    fn nearest_activation_moment(&self) -> Option<DateTime<Utc>>;

    fn take_nearest_planned_flows(&self) -> Vec<FlowID>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum TimeWheelCancelActivationError {
    #[error(transparent)]
    FlowNotPlanned(TimeWheelFlowNotPlannedError),
}

#[derive(Error, Debug)]
#[error("Flow '{flow_id}' not found planned in the time wheel")]
pub struct TimeWheelFlowNotPlannedError {
    pub flow_id: FlowID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
