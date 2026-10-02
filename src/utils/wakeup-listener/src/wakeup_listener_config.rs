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

/// Re-check period while committed rows are held back for an older running
/// transaction: its commit may raise no wakeup, so the listening timeout would
/// be far too long a wait
pub const HELD_BACK_RECHECK_INTERVAL: Duration = Duration::from_millis(20);

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
    // Debounce: each agent in a flow run chain (outbox, flow events, tasks)
    // adds it to the end-to-end latency, so it stays imperceptible, while still
    // merging the signals of one transaction or a few rapid commits.
    // Timeout: on Sqlite it's the longest poll gap, bounding how late writes of
    // other processes are noticed.
    pub fn local_default() -> Self {
        Self {
            min_debounce_interval: Duration::from_millis(20),
            max_listening_timeout: Duration::from_secs(2),
        }
    }

    // Debounce: same latency reasoning as locally.
    // Timeout: Postgres notifications carry the latency, so the timeout is only
    // a safety net against a missed one (e.g. a missing trigger), and a long one
    // keeps idle agents from querying the database needlessly.
    pub fn production_default() -> Self {
        Self {
            min_debounce_interval: Duration::from_millis(20),
            max_listening_timeout: Duration::from_mins(1),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
