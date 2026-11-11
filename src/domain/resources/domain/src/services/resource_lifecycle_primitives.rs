// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::{
    EventSourcedResource,
    ResourceCleanupError,
    ResourceConditionUpdateError,
    ResourceConditionsPatch,
    ResourceID,
    ResourceLifecycleTransitionError,
    ResourceStatusConflictError,
    ResourceVersion,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The lifecycle transitions of one resource type. Each loads the resources,
/// checks the caller's expectations, applies the transition, persists its
/// event and posts the matching lifecycle message.
///
/// The primitives join the caller's transaction and never open one, so a
/// caller can commit a transition together with writes of its own. A rolled
/// back transaction leaves neither the event nor the message behind.
///
/// Every status write posts a `StatusUpdated` message naming the changed
/// conditions; a write that leaves the status as it was posts nothing.
///
/// The batch transitions take each resource at most once; a repeated id fails
/// the whole batch.
#[async_trait::async_trait]
pub trait ResourceLifecyclePrimitives<R: EventSourcedResource>: Send + Sync {
    /// Moves the resource to `Reconciling` and records the generation it
    /// observed. Returns `None`, changing nothing, when the resource does not
    /// need reconciliation, and a resource already reconciling as it is. The
    /// expected status generation, when given, is checked only when the start
    /// is recorded
    async fn begin_reconciliation(
        &self,
        id: &ResourceID,
        expected_status_generation: Option<u64>,
    ) -> Result<Option<R>, ResourceLifecycleTransitionError<R>>;

    /// Moves the resource to `Ready` with the expected generation reconciled
    async fn complete_reconciliation(
        &self,
        id: &ResourceID,
        expected: ResourceVersion,
    ) -> Result<R, ResourceLifecycleTransitionError<R>>;

    /// Moves the resource to `Failed` or `Degraded` with the error as its
    /// `ReconciliationError`
    async fn fail_reconciliation(
        &self,
        id: &ResourceID,
        expected: ResourceVersion,
        error: &R::ReconcileError,
    ) -> Result<R, ResourceLifecycleTransitionError<R>>;

    /// Requests the deletion of the resources and posts one
    /// `DeletionRequested` for all of them. Requesting it again for a resource
    /// being deleted records nothing new, but the message still covers it, so
    /// its cleanup is retried
    async fn request_deletion(
        &self,
        ids: &[ResourceID],
    ) -> Result<Vec<R>, ResourceLifecycleTransitionError<R>>;

    /// Completes the deletion of the resources and posts one `Deleted` for
    /// those it completed
    async fn complete_deletion(
        &self,
        completions: &[ResourceDeletionCompletion],
    ) -> Result<ResourceBatchTransition<R>, ResourceLifecycleTransitionError<R>>;

    /// Records failed cleanups; the deletions stay in progress
    async fn record_cleanup_failure(
        &self,
        failures: &[ResourceCleanupFailure],
    ) -> Result<ResourceBatchTransition<R>, ResourceLifecycleTransitionError<R>>;

    /// Sets and removes the conditions the patch names, each checked against
    /// its registered schema and the change against the type's status rules.
    /// A patch that changes nothing records and posts nothing. Accepted while
    /// the deletion is in progress, so a writer can still record an outcome
    async fn update_conditions(
        &self,
        id: &ResourceID,
        expected_status_generation: u64,
        patch: &ResourceConditionsPatch,
    ) -> Result<R, ResourceConditionUpdateError<R>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceDeletionCompletion {
    pub id: ResourceID,
    /// Completes the deletion only if the status is still at this generation;
    /// `None` completes it whatever was written to the status meanwhile
    pub expected_status_generation: Option<u64>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceCleanupFailure {
    pub id: ResourceID,
    /// The status generation the cleanup started from
    pub expected_status_generation: u64,
    pub error: Arc<ResourceCleanupError>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The outcome of a transition applied to several resources
#[derive(Debug)]
pub struct ResourceBatchTransition<R> {
    /// The resources the transition was recorded for, in their new state
    pub transitioned: Vec<R>,
    /// The resources left as they were
    pub skipped: Vec<ResourceTransitionSkip>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceTransitionSkip {
    pub id: ResourceID,
    pub reason: ResourceTransitionSkipReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceTransitionSkipReason {
    /// The deletion is not waiting for a cleanup: it was completed meanwhile,
    /// or never requested
    NotAwaitingCleanup,
    /// Another writer updated the status since the expected generation
    StatusConflict(ResourceStatusConflictError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
