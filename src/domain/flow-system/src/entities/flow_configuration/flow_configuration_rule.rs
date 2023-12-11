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
    pub throttling_period: Option<Duration>,
    pub minimal_data_batch: Option<i32>,
}

/////////////////////////////////////////////////////////////////////////////////////////
