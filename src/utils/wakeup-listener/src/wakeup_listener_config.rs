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

/// Timing shared by all agents waiting on a [`crate::WakeupListener`]
#[derive(Debug, Clone)]
pub struct WakeupListenerConfig {
    /// How long to absorb a burst of signals after the first one
    pub min_debounce_interval: Duration,

    /// Fallback re-check period, in case a signal is missed
    pub max_listening_timeout: Duration,
}

impl WakeupListenerConfig {
    pub fn local_default() -> Self {
        Self {
            min_debounce_interval: Duration::from_millis(20),
            max_listening_timeout: Duration::from_secs(2),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
