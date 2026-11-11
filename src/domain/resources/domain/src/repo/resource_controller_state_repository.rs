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

use crate::{
    ActionableResourcesQuery,
    ResourceControllerName,
    ResourceControllerState,
    ResourceID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Storage of [`ResourceControllerState`] rows, keyed by controller and
/// resource, and the queries that find work for a controller.
///
/// A resource of the queried schema that is not deleted is **actionable** when
/// one of these predicates holds:
/// - it is not being deleted, and its status records no reconciliation of its current generation;
/// - its deletion was requested and not completed;
/// - the controller reacts to status, and the resource's status generation differs from the handled
///   one (a missing row handled none);
///
/// and the controller's row does not suppress it:
/// - with `not_before` set: suppressed until `not_before`;
/// - without it: suppressed while the resource's generation and status generation equal the handled
///   ones and the row's revision is the controller's current one.
#[async_trait::async_trait]
pub trait ResourceControllerStateRepository: Send + Sync {
    async fn get_state(
        &self,
        controller: &ResourceControllerName,
        resource_id: &ResourceID,
    ) -> Result<Option<ResourceControllerState>, InternalError>;

    /// Inserts the row or replaces the stored one.
    async fn upsert_state(&self, state: &ResourceControllerState) -> Result<(), InternalError>;

    /// Removes the rows of every controller for the resource.
    async fn delete_states_for_resource(
        &self,
        resource_id: &ResourceID,
    ) -> Result<(), InternalError>;

    /// Returns at most `limit` distinct actionable resources, in no
    /// particular order.
    async fn find_actionable_resource_ids(
        &self,
        query: &ActionableResourcesQuery,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<ResourceID>, InternalError>;

    /// The earliest `not_before` among rows whose resource matches a
    /// predicate, past ones included.
    async fn find_next_deadline(
        &self,
        query: &ActionableResourcesQuery,
    ) -> Result<Option<DateTime<Utc>>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
