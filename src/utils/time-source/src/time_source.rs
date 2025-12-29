// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use tokio::sync::oneshot;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Abstracts the system time source
#[async_trait::async_trait]
pub trait SystemTimeSource: Send + Sync {
    fn now(&self) -> DateTime<Utc>;

    async fn sleep(&self, duration: Duration) {
        let std_duration = duration.to_std().unwrap();

        tokio::time::sleep(std_duration).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SystemTimeSource)]
pub struct SystemTimeSourceDefault;

#[async_trait::async_trait]
impl SystemTimeSource for SystemTimeSourceDefault {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SystemTimeSourceStub {
    t: Arc<Mutex<Option<DateTime<Utc>>>>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn SystemTimeSource)]
impl SystemTimeSourceStub {
    pub fn new() -> Self {
        Self {
            t: Arc::new(Mutex::new(None)),
        }
    }

    pub fn new_set(t: DateTime<Utc>) -> Self {
        Self {
            t: Arc::new(Mutex::new(Some(t))),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        *self.t.lock().unwrap() = Some(t);
    }

    pub fn unset(&self) {
        *self.t.lock().unwrap() = None;
    }
}

impl SystemTimeSource for SystemTimeSourceStub {
    fn now(&self) -> DateTime<Utc> {
        match *self.t.lock().unwrap() {
            None => Utc::now(),
            Some(ref t) => *t,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type AwaitingCallerId = u32;

#[derive(Debug)]
struct AwaitingCaller {
    id: AwaitingCallerId,
    wake_up_time: DateTime<Utc>,
    waker_tx: Option<oneshot::Sender<()>>,
}

impl Eq for AwaitingCaller {}

impl PartialEq<Self> for AwaitingCaller {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd<Self> for AwaitingCaller {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AwaitingCaller {
    fn cmp(&self, other: &Self) -> Ordering {
        let first_cmp = self.wake_up_time.cmp(&other.wake_up_time);

        if first_cmp != Ordering::Equal {
            return first_cmp;
        }

        self.id.cmp(&other.id)
    }
}

#[derive(Debug)]
struct FakeSystemTimeSourceState {
    t: DateTime<Utc>,
    next_caller_id: AwaitingCallerId,
    awaiting_callers: BinaryHeap<Reverse<AwaitingCaller>>,
}

#[derive(Debug, Clone)]
pub struct FakeSystemTimeSource {
    pub initial_time: DateTime<Utc>,
    state: Arc<Mutex<FakeSystemTimeSourceState>>,
}

impl FakeSystemTimeSource {
    pub fn new(t: DateTime<Utc>) -> Self {
        let state = FakeSystemTimeSourceState {
            t,
            next_caller_id: 1,
            awaiting_callers: BinaryHeap::new(),
        };

        Self {
            initial_time: t,
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub fn advance(&self, time_quantum: Duration) -> Vec<AwaitingCallerId> {
        let ready_callers = {
            let mut state = self.state.lock().unwrap();

            let new_time = state.t + time_quantum;
            let mut ready_callers = vec![];

            // First, collect all ready callers without advancing time yet
            while let Some(awaiting_caller) = state.awaiting_callers.peek() {
                if awaiting_caller.0.wake_up_time > new_time {
                    break;
                }

                ready_callers.push(state.awaiting_callers.pop().unwrap().0);
            }

            // Only advance time after collecting ready callers
            state.t = new_time;

            ready_callers
        }; // Release lock before sending wakeup signals

        // Send wakeup signals outside of the lock to prevent deadlocks
        let caller_ids: Vec<AwaitingCallerId> = ready_callers
            .into_iter()
            .map(|mut ready_caller| {
                let caller_id = ready_caller.id;
                if let Some(tx) = ready_caller.waker_tx.take() {
                    // Don't panic if receiver is dropped - this can happen during test cleanup
                    let _ = tx.send(());
                }
                caller_id
            })
            .collect();

        caller_ids
    }
}

#[async_trait::async_trait]
impl SystemTimeSource for FakeSystemTimeSource {
    fn now(&self) -> DateTime<Utc> {
        let state = self.state.lock().unwrap();

        state.t
    }

    async fn sleep(&self, duration: Duration) {
        // Handle zero or negative duration immediately
        if duration <= Duration::zero() {
            return;
        }

        let (tx, rx) = oneshot::channel();

        {
            let mut state = self.state.lock().unwrap();

            let new_awaiting_caller = Reverse(AwaitingCaller {
                id: state.next_caller_id,
                wake_up_time: state.t + duration,
                waker_tx: Some(tx),
            });

            state.awaiting_callers.push(new_awaiting_caller);
            state.next_caller_id += 1;
        }

        // Handle the case where receiver might be dropped during test cleanup
        let _ = rx.await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub enum SystemTimeSourceProvider {
    #[default]
    Default,
    Stub(SystemTimeSourceStub),
    Inherited,
}

impl SystemTimeSourceProvider {
    pub fn embed_into_catalog(self, target_catalog_builder: &mut dill::CatalogBuilder) {
        match self {
            SystemTimeSourceProvider::Default => {
                target_catalog_builder.add::<SystemTimeSourceDefault>();
            }
            SystemTimeSourceProvider::Stub(stub) => {
                target_catalog_builder
                    .add_value(stub)
                    .bind::<dyn SystemTimeSource, SystemTimeSourceStub>();
            }
            SystemTimeSourceProvider::Inherited => {
                // No-op
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
