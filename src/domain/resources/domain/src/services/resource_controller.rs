// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;

use crate::{ResourceID, ResourceProcessOutcome};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The main lifecycle controller of one resource type, driven by the
/// controller runtime. Registered with a `ResourceControllerMeta`.
///
/// The runtime asks `process` about every resource that needs attention: one
/// that is not reconciled at its current generation, one whose deletion was
/// requested and not completed, one whose status generation changed when the
/// controller reacts to status, and one listed by `list_extra_actionable`.
/// After `Done`, it asks again only once the resource changes, the controller
/// revision changes or the resource is listed as an extra one.
#[async_trait::async_trait]
pub trait ResourceController: Send + Sync {
    /// Resources needing attention beyond the generic rules, such as tasks
    /// whose claim expired. `process` must take such a resource out of this
    /// set or return `RetryAt`; one that returns `Done` and stays in the set is
    /// asked about at most once per drain.
    async fn list_extra_actionable(
        &self,
        _now: DateTime<Utc>,
        _limit: usize,
    ) -> Result<Vec<ResourceID>, InternalError> {
        Ok(Vec::new())
    }

    /// Does the next step for one resource, composing lifecycle primitives in
    /// short transactions it opens itself. Idempotent: it reads the current
    /// state, so a second call, or a call after a crash, is safe.
    ///
    /// An error is retried with a backoff that grows with each failure.
    async fn process(&self, id: &ResourceID) -> Result<ResourceProcessOutcome, InternalError>;

    /// The nearest moment a resource joins the extra set without any message,
    /// such as the earliest claim expiry
    async fn next_extra_deadline(&self) -> Result<Option<DateTime<Utc>>, InternalError> {
        Ok(None)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
