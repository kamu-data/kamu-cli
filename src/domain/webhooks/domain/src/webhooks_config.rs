// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_MAX_WEBHOOK_CONSECUTIVE_FAILURES: u32 = 5;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct WebhooksConfig {
    pub max_consecutive_failures: u32,
}

impl WebhooksConfig {
    pub fn new(max_consecutive_failures: u32) -> Self {
        Self {
            max_consecutive_failures,
        }
    }
}

impl Default for WebhooksConfig {
    fn default() -> Self {
        Self {
            max_consecutive_failures: DEFAULT_MAX_WEBHOOK_CONSECUTIVE_FAILURES,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
