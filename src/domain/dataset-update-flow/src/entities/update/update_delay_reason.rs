// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateDelayReason {
    Throttling(UpdateDelayReasonThrottling),
    Batching(UpdateDelayReasonBatching),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateDelayReasonThrottling {
    pub interval: Duration,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateDelayReasonBatching {
    pub available_records: usize,
    pub threshold_recods: usize,
}

/////////////////////////////////////////////////////////////////////////////////////////
