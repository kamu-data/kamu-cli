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

/////////////////////////////////////////////////////////////////////////////////////////

/// Abstracts the system time source
#[async_trait::async_trait]
pub trait SystemTimeSource: Send + Sync {
    fn now(&self) -> DateTime<Utc>;

    async fn sleep(&self, duration: Duration);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SystemTimeSource)]
pub struct SystemTimeSourceDefault;

#[async_trait::async_trait]
impl SystemTimeSource for SystemTimeSourceDefault {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn sleep(&self, duration: Duration) {
        let std_duration = duration.to_std().unwrap();

        tokio::time::sleep(std_duration).await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct AwaitingCaller {
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
        self.wake_up_time.cmp(&other.wake_up_time)
    }
}

#[derive(Debug)]
struct SystemTimeSourceStubState {
    t: DateTime<Utc>,
    awaiting_callers: BinaryHeap<Reverse<AwaitingCaller>>,
}

#[derive(Debug, Clone)]
pub struct FakeSystemTimeSource {
    state: Arc<Mutex<SystemTimeSourceStubState>>,
}

impl FakeSystemTimeSource {
    pub fn new() -> Self {
        let state = SystemTimeSourceStubState {
            t: Utc::now(),
            awaiting_callers: BinaryHeap::new(),
        };

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub fn new_set(t: DateTime<Utc>) -> Self {
        let state = SystemTimeSourceStubState {
            t,
            awaiting_callers: BinaryHeap::new(),
        };

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        let mut state = self.state.lock().unwrap();

        if state.t > t {
            panic!(
                "The previous time [{}] is more than new time [{t}]",
                state.t
            );
        }

        state.t = t;

        let mut ready_callers = vec![];

        while let Some(awaiting_caller) = state.awaiting_callers.peek() {
            if awaiting_caller.0.wake_up_time > t {
                break;
            }

            ready_callers.push(state.awaiting_callers.pop().unwrap());
        }

        for mut ready_caller in ready_callers {
            let tx = ready_caller.0.waker_tx.take().unwrap();

            tx.send(()).unwrap();
        }
    }

    pub fn unset(&self) {
        let mut state = self.state.lock().unwrap();

        if !state.awaiting_callers.is_empty() {
            panic!("There are awaiting callers");
        }

        state.t = Utc::now();
    }

    pub fn advance(&self, time_quantum: Duration) {
        let new_t = {
            let state = self.state.lock().unwrap();

            state.t + time_quantum
        };

        self.set(new_t);
    }
}

#[async_trait::async_trait]
impl SystemTimeSource for FakeSystemTimeSource {
    fn now(&self) -> DateTime<Utc> {
        let state = self.state.lock().unwrap();

        state.t
    }

    async fn sleep(&self, duration: Duration) {
        let (tx, rx) = oneshot::channel();

        {
            let mut state = self.state.lock().unwrap();

            let new_awaiting_caller = Reverse(AwaitingCaller {
                wake_up_time: state.t + duration,
                waker_tx: Some(tx),
            });

            state.awaiting_callers.push(new_awaiting_caller);
        }

        rx.await.unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
