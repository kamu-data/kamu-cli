// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::MutexGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Shared by every loop of one controller: the agent's loop and the loops of
/// drain sessions take turns through it, so that they never double-process a
/// resource, and it counts the `process` calls they all made
#[derive(Debug, Default)]
pub(crate) struct ResourceControllerDrainGate {
    turn: tokio::sync::Mutex<()>,
    processed: AtomicUsize,
}

impl ResourceControllerDrainGate {
    /// Waits until no other drain of the controller runs
    pub async fn enter(&self) -> MutexGuard<'_, ()> {
        self.turn.lock().await
    }

    /// `process` calls all drains of the controller made so far
    pub fn processed(&self) -> usize {
        self.processed.load(Ordering::SeqCst)
    }

    pub fn count_processed(&self) {
        self.processed.fetch_add(1, Ordering::SeqCst);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
