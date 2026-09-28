// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::time::Duration;

use kamu_wakeup_listener_inmem::InMemoryWakeupListener;
use wakeup_listener::{WakeHint, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Note: time is paused, so timeouts elapse instantly and deterministically

#[test_log::test(tokio::test(start_paused = true))]
async fn test_times_out_without_signal() {
    let harness = InMemoryWakeupHarness::new();

    assert_matches!(harness.wait_wake(TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_wakes_up_on_signal_while_waiting() {
    let harness = InMemoryWakeupHarness::new();

    let (hint, elapsed) = harness
        .wait_wake_while_signaling_after(Duration::from_millis(100))
        .await;

    assert_matches!(hint, WakeHint::Signaled);
    assert!(elapsed < TIMEOUT, "Woke up by timeout: {elapsed:?}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_signal_before_waiting_is_not_lost() {
    let harness = InMemoryWakeupHarness::new();

    harness.signal();

    assert_matches!(harness.wait_wake(TIMEOUT).await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_signals_before_waiting_coalesce() {
    let harness = InMemoryWakeupHarness::new();

    harness.signal();
    harness.signal();
    harness.signal();

    assert_matches!(harness.wait_wake(TIMEOUT).await, WakeHint::Signaled);
    assert_matches!(harness.wait_wake(TIMEOUT).await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TIMEOUT: Duration = Duration::from_secs(10);

struct InMemoryWakeupHarness {
    listener: InMemoryWakeupListener,
}

impl InMemoryWakeupHarness {
    fn new() -> Self {
        Self {
            listener: InMemoryWakeupListener::new(),
        }
    }

    fn signal(&self) {
        self.listener.signal();
    }

    async fn wait_wake(&self, timeout: Duration) -> WakeHint {
        self.listener
            .wait_wake(timeout, Duration::ZERO)
            .await
            .unwrap()
    }

    async fn signal_after(&self, delay: Duration) {
        tokio::time::sleep(delay).await;
        self.signal();
    }

    async fn wait_wake_while_signaling_after(&self, delay: Duration) -> (WakeHint, Duration) {
        let started_at = tokio::time::Instant::now();
        let (hint, ()) = tokio::join!(self.wait_wake(TIMEOUT), self.signal_after(delay));
        (hint, started_at.elapsed())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
