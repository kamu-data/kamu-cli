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
        let mut state = self.state.lock().unwrap();

        state.t += time_quantum;

        let mut ready_callers = vec![];

        while let Some(awaiting_caller) = state.awaiting_callers.peek() {
            if awaiting_caller.0.wake_up_time > state.t {
                break;
            }

            ready_callers.push(state.awaiting_callers.pop().unwrap().0);
        }

        for ready_caller in &mut ready_callers {
            let tx = ready_caller.waker_tx.take().unwrap();

            tx.send(()).unwrap();
        }

        ready_callers
            .into_iter()
            .map(|ready_caller| ready_caller.id)
            .collect()
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
                id: state.next_caller_id,
                wake_up_time: state.t + duration,
                waker_tx: Some(tx),
            });

            state.awaiting_callers.push(new_awaiting_caller);
            state.next_caller_id += 1;
        }

        rx.await.unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
