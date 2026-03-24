// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{ConcurrentModificationError, EventID};
use internal_error::{ErrorIntoInternal, InternalError};

use crate::domain::{
    DeclarativeResource,
    InvariantViolationOf,
    ReconcilableEventSourcedResource,
    ResourceDescriptorProvider,
    ResourceRepository,
    ResourceStatusLike,
    UpdateResourceError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) enum SyncResourceSnapshotError {
    ConcurrentModification(ConcurrentModificationError),
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn sync_resource_snapshot<R>(
    resource_repository: &dyn ResourceRepository,
    resource: &R,
    expected_last_event_id: Option<EventID>,
) -> Result<(), SyncResourceSnapshotError>
where
    R: ReconcilableEventSourcedResource + ResourceDescriptorProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: serde::Serialize,
    R::Status: serde::Serialize + ResourceStatusLike,
{
    let snapshot = resource
        .make_resource_snapshot()
        .map_err(SyncResourceSnapshotError::Internal)?;

    match resource_repository
        .update_resource(&snapshot, expected_last_event_id)
        .await
    {
        Ok(()) => Ok(()),
        Err(UpdateResourceError::ConcurrentModification(err)) => {
            Err(SyncResourceSnapshotError::ConcurrentModification(err))
        }
        Err(err) => Err(SyncResourceSnapshotError::Internal(err.int_err())),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
