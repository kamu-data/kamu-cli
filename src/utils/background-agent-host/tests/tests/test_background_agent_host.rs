// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use background_agent_host::{BackgroundAgent, BackgroundAgentRestartReason, CancellationToken};
use chrono::TimeDelta;
use internal_error::InternalError;
use pretty_assertions::assert_eq;
use tokio::sync::Notify;

use crate::tests::utils::{EventCounter, HostHarness};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const AGENT_A: &str = "test.AgentA";
const AGENT_B: &str = "test.AgentB";

#[test_log::test(tokio::test)]
async fn test_failing_agent_restarts_with_growing_backoff_while_another_keeps_running() {
    let a = ScriptedAgent::new(AGENT_A, [ScriptedRun::Fail, ScriptedRun::Fail]);
    let b = ScriptedAgent::new(AGENT_B, []);
    let harness = HostHarness::new([a.clone(), b.clone()]);
    let mut host = harness.start();

    a.wait_for_runs(1).await;
    harness.settle().await;
    assert_eq!(
        harness.restarts(AGENT_A, BackgroundAgentRestartReason::Failed),
        1
    );
    assert_eq!(harness.running(AGENT_A), 0);
    assert!(harness.last_failed_now(AGENT_A));
    assert_eq!(harness.running(AGENT_B), 1);

    // The first restart waits the minimum delay, the second twice as long
    harness.advance(TimeDelta::seconds(1)).await;
    a.wait_for_runs(2).await;
    harness.settle().await;
    assert_eq!(
        harness.restarts(AGENT_A, BackgroundAgentRestartReason::Failed),
        2
    );

    harness.advance(TimeDelta::seconds(1)).await;
    assert_eq!(a.runs(), 2);

    harness.advance(TimeDelta::seconds(1)).await;
    a.wait_for_runs(3).await;
    harness.settle().await;
    assert_eq!(harness.running(AGENT_A), 1);
    assert_eq!(b.runs(), 1);

    host.shutdown(&harness).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_panicking_agent_restarts() {
    let a = ScriptedAgent::new(AGENT_A, [ScriptedRun::Panic]);
    let harness = HostHarness::new([a.clone()]);
    let mut host = harness.start();

    a.wait_for_runs(1).await;
    harness.settle().await;
    assert_eq!(
        harness.restarts(AGENT_A, BackgroundAgentRestartReason::Panicked),
        1
    );
    assert!(harness.last_failed_now(AGENT_A));

    harness.advance(TimeDelta::seconds(1)).await;
    a.wait_for_runs(2).await;

    host.shutdown(&harness).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_agent_returning_before_a_shutdown_restarts() {
    let a = ScriptedAgent::new(AGENT_A, [ScriptedRun::Return]);
    let harness = HostHarness::new([a.clone()]);
    let mut host = harness.start();

    a.wait_for_runs(1).await;
    harness.settle().await;
    assert_eq!(
        harness.restarts(AGENT_A, BackgroundAgentRestartReason::Completed),
        1
    );
    // Not a failure
    assert!(harness.never_failed(AGENT_A));

    harness.advance(TimeDelta::seconds(1)).await;
    a.wait_for_runs(2).await;

    host.shutdown(&harness).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_run_lasting_the_longest_delay_resets_the_backoff() {
    let a = ScriptedAgent::new(
        AGENT_A,
        [
            ScriptedRun::Fail,
            ScriptedRun::Fail,
            ScriptedRun::ReturnWhenReleased,
        ],
    );
    let harness = HostHarness::new([a.clone()]);
    let mut host = harness.start();

    a.wait_for_runs(1).await;
    harness.settle().await;
    harness.advance(TimeDelta::seconds(1)).await;
    a.wait_for_runs(2).await;
    harness.settle().await;
    harness.advance(TimeDelta::seconds(2)).await;
    a.wait_for_runs(3).await;

    // A third consecutive restart would wait 4s; after a healthy run, 1s
    harness.advance(HostHarness::MAX_RESTART_DELAY).await;
    a.release();
    harness.settle().await;
    harness.advance(TimeDelta::seconds(1)).await;
    a.wait_for_runs(4).await;

    host.shutdown(&harness).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_fatal_failure_stops_every_agent_and_ends_the_host() {
    let a = ScriptedAgent::new_fatal(AGENT_A, [ScriptedRun::Fail]);
    let b = ScriptedAgent::new(AGENT_B, []);
    let harness = HostHarness::new([a.clone(), b.clone()]);
    let mut host = harness.start();

    let result = host.wait_for_end().await;

    assert_eq!(
        result.unwrap_err().reason(),
        "Internal error: scripted failure"
    );
    assert!(b.stopped_on_shutdown());
    assert_eq!(a.runs(), 1);
    assert_eq!(
        harness.restarts(AGENT_A, BackgroundAgentRestartReason::Failed),
        0
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_fatal_failure_is_returned_even_with_no_time_to_stop() {
    let a = ScriptedAgent::new_fatal(AGENT_A, [ScriptedRun::Fail]);
    let b = ScriptedAgent::new(AGENT_B, [ScriptedRun::IgnoreShutdown]);
    let harness = HostHarness::with_shutdown_timeout([a.clone(), b.clone()], TimeDelta::zero());
    let mut host = harness.start();

    let result = host.wait_for_end().await;

    assert_eq!(
        result.unwrap_err().reason(),
        "Internal error: scripted failure"
    );
    assert!(b.was_aborted());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_shutdown_waits_for_the_current_step() {
    let a = ScriptedAgent::new(AGENT_A, [ScriptedRun::ReturnWhenReleased]);
    let harness = HostHarness::new([a.clone()]);
    let mut host = harness.start();
    a.wait_for_runs(1).await;

    harness.request_shutdown();
    harness.settle().await;
    harness.advance(TimeDelta::seconds(9)).await;
    assert!(!host.has_ended());

    a.release();
    host.wait_for_end().await.unwrap();
    assert!(!a.was_aborted());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_agent_failing_during_a_shutdown_is_not_restarted() {
    let a = ScriptedAgent::new(AGENT_A, [ScriptedRun::FailWhenReleased]);
    let harness = HostHarness::new([a.clone()]);
    let mut host = harness.start();
    a.wait_for_runs(1).await;

    harness.request_shutdown();
    harness.settle().await;
    a.release();

    host.wait_for_end().await.unwrap();
    assert_eq!(a.runs(), 1);
    assert!(harness.last_failed_now(AGENT_A));
    assert_eq!(
        harness.restarts(AGENT_A, BackgroundAgentRestartReason::Failed),
        0
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_shutdown_aborts_agents_still_running_after_the_bound() {
    let a = ScriptedAgent::new(AGENT_A, [ScriptedRun::IgnoreShutdown]);
    let b = ScriptedAgent::new(AGENT_B, []);
    let harness = HostHarness::new([a.clone(), b.clone()]);
    let mut host = harness.start();
    a.wait_for_runs(1).await;

    harness.request_shutdown();
    harness.settle().await;
    harness.advance(HostHarness::SHUTDOWN_TIMEOUT).await;

    host.wait_for_end().await.unwrap();
    assert!(a.was_aborted());
    assert_eq!(harness.running(AGENT_A), 0);
    assert!(b.stopped_on_shutdown());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_shutdown_ends_a_restart_backoff() {
    let a = ScriptedAgent::new(AGENT_A, [ScriptedRun::Fail]);
    let harness = HostHarness::new([a.clone()]);
    let mut host = harness.start();
    a.wait_for_runs(1).await;
    harness.settle().await;

    // Without moving the clock
    host.shutdown(&harness).await.unwrap();

    assert_eq!(a.runs(), 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
#[should_panic(expected = "No background agents found")]
async fn test_host_without_agents_panics() {
    let harness = HostHarness::without_agents();
    harness.host.run(CancellationToken::new()).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What one run of a `ScriptedAgent` does
#[derive(Debug, Clone, Copy)]
enum ScriptedRun {
    /// Runs until a shutdown, then returns
    UntilShutdown,
    Fail,
    Panic,
    /// Returns at once, before any shutdown
    Return,
    /// Ignores the shutdown until `ScriptedAgent::release`, then returns: a
    /// step in progress
    ReturnWhenReleased,
    /// Like `ReturnWhenReleased`, but fails
    FailWhenReleased,
    /// Never returns
    IgnoreShutdown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs its script, one entry per run, then runs until a shutdown
struct ScriptedAgent {
    name: &'static str,
    fatal: bool,
    script: Mutex<VecDeque<ScriptedRun>>,
    runs: EventCounter,
    released: Notify,
    stopped_on_shutdown: AtomicBool,
    aborted: Arc<AtomicBool>,
}

impl ScriptedAgent {
    fn new<const N: usize>(name: &'static str, script: [ScriptedRun; N]) -> Arc<Self> {
        Self::build(name, false, script)
    }

    fn new_fatal<const N: usize>(name: &'static str, script: [ScriptedRun; N]) -> Arc<Self> {
        Self::build(name, true, script)
    }

    fn build<const N: usize>(
        name: &'static str,
        fatal: bool,
        script: [ScriptedRun; N],
    ) -> Arc<Self> {
        Arc::new(Self {
            name,
            fatal,
            script: Mutex::new(script.into()),
            runs: EventCounter::new(),
            released: Notify::new(),
            stopped_on_shutdown: AtomicBool::new(false),
            aborted: Arc::new(AtomicBool::new(false)),
        })
    }

    fn runs(&self) -> usize {
        self.runs.get()
    }

    async fn wait_for_runs(&self, total: usize) {
        self.runs
            .wait_for(total, format!("runs of {}", self.name))
            .await;
    }

    fn release(&self) {
        self.released.notify_one();
    }

    fn stopped_on_shutdown(&self) -> bool {
        self.stopped_on_shutdown.load(Ordering::SeqCst)
    }

    fn was_aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
    }

    fn next_run(&self) -> ScriptedRun {
        self.script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(ScriptedRun::UntilShutdown)
    }
}

#[async_trait::async_trait]
impl BackgroundAgent for ScriptedAgent {
    fn agent_name(&self) -> &'static str {
        self.name
    }

    async fn run(&self, shutdown: CancellationToken) -> Result<(), InternalError> {
        let run = self.next_run();
        self.runs.increment();

        match run {
            ScriptedRun::UntilShutdown => {
                shutdown.cancelled().await;
                self.stopped_on_shutdown.store(true, Ordering::SeqCst);
                Ok(())
            }
            ScriptedRun::Fail => Err(InternalError::new("scripted failure")),
            ScriptedRun::Panic => panic!("scripted panic"),
            ScriptedRun::Return => Ok(()),
            ScriptedRun::ReturnWhenReleased => {
                self.released.notified().await;
                Ok(())
            }
            ScriptedRun::FailWhenReleased => {
                self.released.notified().await;
                Err(InternalError::new("scripted failure"))
            }
            ScriptedRun::IgnoreShutdown => {
                let _guard = AbortGuard(self.aborted.clone());
                std::future::pending().await
            }
        }
    }

    fn is_failure_fatal(&self) -> bool {
        self.fatal
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Marks the agent aborted when its run is dropped before returning
struct AbortGuard(Arc<AtomicBool>);

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
