// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use async_utils::join_error_message;
use tokio::task::{JoinError, JoinSet};
use tracing::Instrument as _;

use crate::controller_runtime::{
    ResourceControllerCandidate,
    ResourceControllerRegistration,
    ResourceProcessAttempt,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the controller's `process` for the candidates of one pass, at most
/// `concurrency` at a time.
///
/// Every call runs in a tokio task of its own. That is what turns a panic in
/// one controller call into the failure of that one resource: it is recorded
/// and backed off like an error, instead of tearing down the loop and making
/// the resource come back first on the next attempt, forever.
pub(crate) struct ResourceProcessExecutor<'a> {
    registration: &'a ResourceControllerRegistration,
    concurrency: usize,
}

impl<'a> ResourceProcessExecutor<'a> {
    pub fn new(registration: &'a ResourceControllerRegistration, concurrency: usize) -> Self {
        Self {
            registration,
            concurrency,
        }
    }

    /// Processes every candidate and returns how each attempt ended, in
    /// completion order. Never fails: an error or a panic is an attempt
    /// outcome.
    pub async fn run(
        &self,
        candidates: Vec<ResourceControllerCandidate>,
    ) -> Vec<(ResourceControllerCandidate, ResourceProcessAttempt)> {
        let mut pending = candidates.into_iter();
        let mut running = JoinSet::new();
        // A panicked task returns only its task ID, so the candidate is kept
        // aside to know which resource failed
        let mut running_candidates = HashMap::new();
        let mut finished = Vec::new();

        loop {
            // Top up to the concurrency limit
            while running.len() < self.concurrency {
                let Some(candidate) = pending.next() else {
                    break;
                };
                let abort_handle = running.spawn(self.process(candidate));
                running_candidates.insert(abort_handle.id(), candidate);
            }

            // Wait for the next call to end; none left running means all ended
            let Some(joined) = running.join_next_with_id().await else {
                return finished;
            };

            let (task_id, attempt) = match joined {
                Ok((task_id, attempt)) => (task_id, attempt),
                Err(join_error) => (join_error.id(), self.panicked(join_error)),
            };
            let candidate = running_candidates
                .remove(&task_id)
                .expect("Every spawned task has its candidate");
            finished.push((candidate, attempt));
        }
    }

    /// One `process` call, with an error turned into a failed attempt
    fn process(
        &self,
        candidate: ResourceControllerCandidate,
    ) -> impl Future<Output = ResourceProcessAttempt> + Send + 'static {
        let controller = self.registration.controller().clone();
        let controller_name = self.registration.meta().controller_name.as_str();
        let span = tracing::info_span!(
            "ResourceController::process",
            controller = controller_name,
            resource_id = %candidate.id(),
        );

        async move {
            match controller.process(&candidate.id()).await {
                Ok(outcome) => ResourceProcessAttempt::Processed(outcome),
                Err(err) => {
                    tracing::error!(
                        error = ?err,
                        error_msg = %err,
                        "Resource processing failed"
                    );
                    ResourceProcessAttempt::Failed(err.reason())
                }
            }
        }
        .instrument(span)
    }

    /// A task that did not return. The runtime never aborts its tasks, so
    /// this is a panic.
    fn panicked(&self, join_error: JoinError) -> ResourceProcessAttempt {
        let message = join_error_message(join_error);

        tracing::error!(
            controller = self.registration.meta().controller_name.as_str(),
            panic = %message,
            "Resource processing panicked"
        );
        ResourceProcessAttempt::Failed(format!("Resource processing panicked: {message}"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
