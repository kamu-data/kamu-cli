// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;

use crate::Schedule;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowConfigurationRule {
    Schedule(Schedule),
    StartCondition(StartConditionConfiguration),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartConditionConfiguration {
    // TODO: throttling period to be removed, using global unconfigurable throttling period instead
    pub throttling_period: Option<Duration>,
    // TODO: modeling to be refined:
    //   - min records to accumulate
    //   - max records to take
    //   - max batching interval
    pub minimal_data_batch: Option<i32>,
}

/////////////////////////////////////////////////////////////////////////////////////////
