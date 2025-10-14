// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use chrono::Duration;

pub const DEFAULT_MAX_WEBHOOK_CONSECUTIVE_FAILURES: u32 = 5;
pub const DEFAULT_WEBHOOK_DELIVERY_TIMEOUT: u32 = 10;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct WebhooksConfig {
    pub max_consecutive_failures: u32,
    pub delivery_timeout: Duration,
}

impl WebhooksConfig {
    pub fn new(max_consecutive_failures: u32, delivery_timeout: Duration) -> Self {
        Self {
            max_consecutive_failures,
            delivery_timeout,
        }
    }
}

impl Default for WebhooksConfig {
    fn default() -> Self {
        Self {
            max_consecutive_failures: DEFAULT_MAX_WEBHOOK_CONSECUTIVE_FAILURES,
            delivery_timeout: Duration::seconds(i64::from(DEFAULT_WEBHOOK_DELIVERY_TIMEOUT)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
