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
    BatchingRule(BatchingRule),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchingRule {
    pub min_records_awaited: u64,
    pub max_records_taken: Option<u64>,
    pub max_batching_interval: Option<Duration>,
}

/////////////////////////////////////////////////////////////////////////////////////////
