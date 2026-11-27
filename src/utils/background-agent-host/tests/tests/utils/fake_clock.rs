// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use observability::metrics::unix_timestamp_seconds;
use time_source::{FakeSystemTimeSource, SystemTimeSource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The fake clock the code under test runs on: its waits pass only when a
/// test moves it
#[derive(Clone)]
pub struct FakeClock {
    time_source: FakeSystemTimeSource,
}

impl FakeClock {
    pub fn new() -> Self {
        Self {
            time_source: FakeSystemTimeSource::new(
                Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap(),
            ),
        }
    }

    pub fn time_source(&self) -> &FakeSystemTimeSource {
        &self.time_source
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.time_source.now()
    }

    /// Real time, for spawned tasks to reach their next wait
    pub async fn settle(&self) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    /// Moves the clock, then lets the tasks react
    pub async fn advance(&self, delta: TimeDelta) {
        self.time_source.advance(delta);
        self.settle().await;
    }

    /// A timestamp gauge holds the clock's current time
    pub fn is_now(&self, timestamp_seconds: f64) -> bool {
        (timestamp_seconds - unix_timestamp_seconds(self.now())).abs() < 1e-3
    }

    /// A timestamp gauge was never set: it reads zero
    pub fn is_unset(timestamp_seconds: f64) -> bool {
        timestamp_seconds.abs() < 1e-3
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
