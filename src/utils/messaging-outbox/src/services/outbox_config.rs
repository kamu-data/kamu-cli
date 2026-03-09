// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxAgentConfig {
    pub min_debounce_interval: Duration,
    pub max_listening_timeout: Duration,
    pub batch_size: usize,
}

impl OutboxAgentConfig {
    pub fn local_default() -> Self {
        Self {
            min_debounce_interval: Duration::from_millis(100),
            max_listening_timeout: Duration::from_secs(2),
            batch_size: 20,
        }
    }

    pub fn production_default() -> Self {
        Self {
            min_debounce_interval: Duration::from_millis(100),
            max_listening_timeout: Duration::from_secs(60),
            batch_size: 100,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
