// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use tokio::task::JoinSet;

use crate::join_error_message;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A future run in a tokio task of its own, so that a panic ends the task, not
/// its caller. Dropping it aborts the task.
pub struct IsolatedTask<T> {
    running: JoinSet<T>,
}

impl<T: Send + 'static> IsolatedTask<T> {
    pub fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        let mut running = JoinSet::new();
        running.spawn(future);
        Self { running }
    }

    /// Waits for the task to end: its output, or the message of its panic.
    /// Cancel safe.
    pub async fn join(&mut self) -> Result<T, String> {
        match self.running.join_next().await {
            Some(Ok(output)) => Ok(output),
            Some(Err(join_error)) => Err(join_error_message(join_error)),
            None => panic!("the task was already joined"),
        }
    }

    /// Aborts the task and waits until it has ended, so that nothing it holds
    /// outlives this call
    pub async fn abort(mut self) {
        self.running.abort_all();
        self.running.join_next().await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
