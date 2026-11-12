// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::domain::{EventSourcedResource, ResourceCleanupFailure, SimpleResourceDeletionError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The error that rolls back the transaction of a cleanup. A failed cleanup
/// discards whatever the reconciler wrote before failing; its failure is then
/// recorded in a transaction of its own.
pub enum ResourceCleanupTransactionError<R: EventSourcedResource> {
    CleanupFailed(ResourceCleanupFailure),
    Deletion(SimpleResourceDeletionError<R>),
}

impl<R: EventSourcedResource> From<InternalError> for ResourceCleanupTransactionError<R> {
    fn from(err: InternalError) -> Self {
        Self::Deletion(SimpleResourceDeletionError::Internal(err))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
