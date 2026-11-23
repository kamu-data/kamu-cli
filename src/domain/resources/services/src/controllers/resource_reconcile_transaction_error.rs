// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::domain::{EventSourcedResource, SimpleResourceReconciliationError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The error that rolls back the transaction of a reconciliation. A failed
/// `reconcile` discards whatever the reconciler wrote before failing; its
/// failure is then recorded in a transaction of its own, since the database
/// may refuse every further statement of the one the reconciler failed in.
pub enum ResourceReconcileTransactionError<R: EventSourcedResource> {
    ReconcileFailed(R::ReconcileError),
    Reconciliation(SimpleResourceReconciliationError<R>),
}

impl<R: EventSourcedResource> From<InternalError> for ResourceReconcileTransactionError<R> {
    fn from(err: InternalError) -> Self {
        Self::Reconciliation(SimpleResourceReconciliationError::Internal(err))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
