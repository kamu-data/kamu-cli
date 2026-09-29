// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;

use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use wakeup_listener::{HubWakeupListener, WakeHint, WakeupListener, WakeupListenerMetrics};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_subscription_signals_then_times_out() {
    let harness = InMemoryWakeupHarness::new();

    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_waits_are_recorded_per_agent() {
    let harness = InMemoryWakeupHarness::new();
    assert!(!harness.has_recorded_wait(AGENT));

    harness.start_listening().await;
    assert!(harness.has_recorded_wait(AGENT));
    assert!(!harness.has_recorded_wait(OTHER_AGENT));

    harness.subscribe(OTHER_CHANNEL).await;
    assert!(harness.has_recorded_wait(OTHER_AGENT));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_dropped_wait_is_recorded() {
    let harness = InMemoryWakeupHarness::new();
    harness.start_listening().await;
    harness.forget_recorded_wait(AGENT);

    // Like an agent racing the wait against its own deadline, which wins
    tokio::select! {
        _ = harness.wait_wake() => panic!("Nothing was signaled"),
        () = tokio::time::sleep(TIMEOUT / 10) => {}
    }

    assert!(harness.has_recorded_wait(AGENT));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_signal_while_waiting_wakes_up() {
    let harness = InMemoryWakeupHarness::new();
    harness.start_listening().await;

    let (hint, ()) = tokio::join!(harness.wait_wake(), harness.signal_after(TIMEOUT / 2));

    assert_matches!(hint, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_signal_before_wait_is_kept() {
    let harness = InMemoryWakeupHarness::new();
    harness.start_listening().await;

    harness.signal(CHANNEL);

    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_signals_coalesce_into_one_wakeup() {
    let harness = InMemoryWakeupHarness::new();
    harness.start_listening().await;

    harness.signal(CHANNEL);
    harness.signal(CHANNEL);
    harness.signal(CHANNEL);

    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_routes_signals_per_channel() {
    let harness = InMemoryWakeupHarness::new();
    harness.start_listening().await;
    let other = harness.subscribe(OTHER_CHANNEL).await;

    harness.signal(OTHER_CHANNEL);

    assert_matches!(
        InMemoryWakeupHarness::wait_wake_on(&other).await,
        WakeHint::Signaled
    );
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_signal_wakes_up_all_listeners_of_channel() {
    let harness = InMemoryWakeupHarness::new();
    harness.start_listening().await;
    let another = harness.subscribe(CHANNEL).await;

    harness.signal(CHANNEL);

    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);
    assert_matches!(
        InMemoryWakeupHarness::wait_wake_on(&another).await,
        WakeHint::Signaled
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(start_paused = true))]
async fn test_late_subscriber_is_signaled_immediately() {
    let harness = InMemoryWakeupHarness::new();

    // Nobody is subscribed yet, so the hub keeps nothing for this signal
    harness.signal(CHANNEL);

    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CHANNEL: &str = "test_channel";
const OTHER_CHANNEL: &str = "test_other_channel";
const TIMEOUT: Duration = Duration::from_secs(1);
const DEBOUNCE_INTERVAL: Duration = Duration::from_millis(20);

const AGENT: &str = "test_agent";
const OTHER_AGENT: &str = "test_other_agent";

type Listener = HubWakeupListener<InMemoryWakeupHub>;

/// Owns a hub and a default listener on [`CHANNEL`]
struct InMemoryWakeupHarness {
    hub: Arc<InMemoryWakeupHub>,
    metrics: Arc<WakeupListenerMetrics>,
    listener: Listener,
}

impl InMemoryWakeupHarness {
    fn new() -> Self {
        let metrics = Arc::new(WakeupListenerMetrics::new());
        let hub = Arc::new(InMemoryWakeupHub::new(metrics.clone()));
        let listener = HubWakeupListener::new(hub.clone(), CHANNEL, AGENT);
        Self {
            hub,
            metrics,
            listener,
        }
    }

    /// The default listener subscribes lazily on its first wait
    async fn start_listening(&self) {
        assert_matches!(self.wait_wake().await, WakeHint::Signaled);
    }

    async fn subscribe(&self, channel: &'static str) -> Listener {
        let listener = HubWakeupListener::new(self.hub.clone(), channel, OTHER_AGENT);
        assert_matches!(Self::wait_wake_on(&listener).await, WakeHint::Signaled);
        listener
    }

    async fn wait_wake(&self) -> WakeHint {
        Self::wait_wake_on(&self.listener).await
    }

    async fn wait_wake_on(listener: &Listener) -> WakeHint {
        listener
            .wait_wake(TIMEOUT, DEBOUNCE_INTERVAL)
            .await
            .unwrap()
    }

    fn has_recorded_wait(&self, agent_name: &str) -> bool {
        self.metrics
            .last_wait_timestamp_seconds
            .with_label_values(&[agent_name])
            .get()
            > 0.0
    }

    fn forget_recorded_wait(&self, agent_name: &str) {
        self.metrics
            .last_wait_timestamp_seconds
            .with_label_values(&[agent_name])
            .set(0.0);
    }

    fn signal(&self, channel: &'static str) {
        self.hub.signal(channel);
    }

    async fn signal_after(&self, delay: Duration) {
        tokio::time::sleep(delay).await;
        self.signal(CHANNEL);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
