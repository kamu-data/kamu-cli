// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use tokio::sync::oneshot;

/////////////////////////////////////////////////////////////////////////////////////////

/// Abstracts the system time source
#[async_trait::async_trait]
pub trait SystemTimeSource: Send + Sync {
    fn now(&self) -> DateTime<Utc>;

    async fn wake_after(&self, duration: Duration);
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

    async fn wake_after(&self, duration: Duration) {
        let std_duration = duration.to_std().unwrap();

        tokio::time::sleep(std_duration).await;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

type AwaitingCallerId = usize;
type WokeUpCallersIds = Vec<AwaitingCallerId>;

#[derive(Debug)]
struct AwaitingCaller {
    id: AwaitingCallerId,
    time_to_wake_up: Duration,
    waker_tx: Option<oneshot::Sender<()>>,
}

#[derive(Debug, Default)]
struct SystemTimeSourceStubState {
    t: Option<DateTime<Utc>>,
    next_caller_id: AwaitingCallerId,
    awaiting_callers: Vec<AwaitingCaller>,
}

#[derive(Debug, Clone)]
pub struct SystemTimeSourceStub {
    state: Arc<Mutex<SystemTimeSourceStubState>>,
}

impl SystemTimeSourceStub {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(SystemTimeSourceStubState::default())),
        }
    }

    pub fn new_set(t: DateTime<Utc>) -> Self {
        let state = SystemTimeSourceStubState {
            t: Some(t),
            next_caller_id: 1,
            awaiting_callers: vec![],
        };

        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub fn set(&self, t: DateTime<Utc>) {
        let mut state = self.state.lock().unwrap();

        state.t = Some(t);
    }

    pub fn unset(&self) {
        let mut state = self.state.lock().unwrap();

        state.t = None;
    }

    pub fn advance(&self, time_quantum: Duration) -> WokeUpCallersIds {
        let mut state = self.state.lock().unwrap();
        let mut woke_up_callers_ids = vec![];

        state.awaiting_callers.retain_mut(|awaiting_caller| {
            awaiting_caller.time_to_wake_up =
                Duration::zero().max(awaiting_caller.time_to_wake_up - time_quantum);

            let is_time_up = awaiting_caller.time_to_wake_up.is_zero();

            if is_time_up {
                woke_up_callers_ids.push(awaiting_caller.id);

                let tx = awaiting_caller.waker_tx.take().unwrap();

                tx.send(()).unwrap();
            }

            !is_time_up
        });

        if let Some(ref mut t) = state.t {
            *t += time_quantum;
        }

        woke_up_callers_ids
    }
}

#[async_trait::async_trait]
impl SystemTimeSource for SystemTimeSourceStub {
    fn now(&self) -> DateTime<Utc> {
        let state = self.state.lock().unwrap();

        match state.t {
            None => Utc::now(),
            Some(ref t) => *t,
        }
    }

    async fn wake_after(&self, duration: Duration) {
        let (tx, rx) = oneshot::channel();

        {
            let mut state = self.state.lock().unwrap();

            let new_awaiting_caller = AwaitingCaller {
                id: state.next_caller_id,
                time_to_wake_up: duration,
                waker_tx: Some(tx),
            };

            state.awaiting_callers.push(new_awaiting_caller);
            state.next_caller_id += 1;
        }

        rx.await.unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
