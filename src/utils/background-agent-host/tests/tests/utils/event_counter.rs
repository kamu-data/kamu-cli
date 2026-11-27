// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Counts the events of a test double, such as its runs, for a test to wait on
pub struct EventCounter {
    count: AtomicUsize,
    happened: Notify,
}

impl EventCounter {
    pub fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            happened: Notify::new(),
        }
    }

    pub fn get(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    pub fn increment(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.happened.notify_waiters();
    }

    /// Waits until `total` events happened. Real time, bounded, so that an
    /// event that never comes fails the test, naming the events `what` counts
    pub async fn wait_for(&self, total: usize, what: impl Display) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.get() < total {
                self.happened.notified().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {total} {what}, got {}", self.get()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
