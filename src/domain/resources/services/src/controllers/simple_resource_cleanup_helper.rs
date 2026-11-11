// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::domain::{
    EventSourcedResource,
    ResourceAggregateLoader,
    ResourceID,
    ResourceLoadError,
    SimpleResourceDeletionError,
    SimpleResourceReconciler,
};
use crate::{ResourceCleanupAttempt, ResourceCleanupTransactionError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the [`SimpleResourceReconciler`] cleanup of a resource whose deletion
/// was requested, within the transaction of its caller
pub struct SimpleResourceCleanupHelper<'a, R>
where
    R: EventSourcedResource,
{
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    reconciler: &'a dyn SimpleResourceReconciler<R>,
}

impl<'a, R> SimpleResourceCleanupHelper<'a, R>
where
    R: EventSourcedResource,
{
    pub fn new(
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        reconciler: &'a dyn SimpleResourceReconciler<R>,
    ) -> Self {
        Self {
            resource_aggregate_loader,
            reconciler,
        }
    }

    /// Runs the cleanup of one resource. A failed cleanup is returned as an
    /// error, so that the transaction of the caller rolls back. Returns `None`
    /// when the resource does not wait for a cleanup.
    pub async fn cleanup(
        &self,
        id: ResourceID,
    ) -> Result<Option<ResourceCleanupAttempt>, ResourceCleanupTransactionError<R>> {
        let resource = self
            .resource_aggregate_loader
            .load(&id)
            .await
            .map_err(ResourceLoadError)
            .map_err(SimpleResourceDeletionError::LoadFailed)
            .map_err(ResourceCleanupTransactionError::Deletion)?;
        if !resource.needs_cleanup() {
            return Ok(None);
        }

        let status_generation = resource.headers().status_generation;
        match self.reconciler.cleanup(&resource).await {
            Ok(()) => Ok(Some(ResourceCleanupAttempt {
                id,
                status_generation,
                failure: None,
            })),
            Err(err) => {
                tracing::warn!(
                    resource_id = %id,
                    error = %err,
                    "Cleanup of a resource being deleted failed"
                );
                Err(ResourceCleanupTransactionError::CleanupFailed(
                    ResourceCleanupAttempt {
                        id,
                        status_generation,
                        failure: Some(Arc::new(err)),
                    },
                ))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
