// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct OutboxConfig {
    /// Defines discretion for main scheduling loop: how often new data is
    /// checked and processed
    pub awaiting_step: Duration,
    /// Defines maximum number of messages attempted to read in 1 step
    pub batch_size: i64,
}

impl OutboxConfig {
    pub fn new(awaiting_step: Duration, batch_size: i64) -> Self {
        Self {
            awaiting_step,
            batch_size,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
