// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_resources::{
    DeclarativeResource,
    ReconcileError,
    ReconcileResourceUseCase,
    ReconcileResourceUseCaseError,
    Reconciler,
    SecretSetEventStore,
    SecretSetID,
    SecretSetResource,
    SecretSetState,
    SecretSetStats,
    needs_reconciliation,
};
use time_source::SystemTimeSource;

use crate::{SecretSetReconcileError, SecretSetReconcileSuccess};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SecretSetReconcileResourceUseCaseImpl {
    event_store: Arc<dyn SecretSetEventStore>,
    reconciler: Arc<
        dyn Reconciler<
                SecretSetResource,
                Success = SecretSetReconcileSuccess,
                Error = SecretSetReconcileError,
            >,
    >,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ReconcileResourceUseCase<SecretSetState> for SecretSetReconcileResourceUseCaseImpl {
    async fn execute(&self, id: &SecretSetID) -> Result<(), ReconcileResourceUseCaseError> {
        let mut resource = SecretSetResource::load(id, self.event_store.as_ref())
            .await
            .int_err()?;
        if needs_reconciliation(resource.as_ref()) {
            return Ok(());
        }

        resource
            .try_mark_reconciliation_started(self.time_source.now())
            .int_err()?;
        resource.save(self.event_store.as_ref()).await.int_err()?;

        match self.reconciler.reconcile(&resource).await {
            Ok(success) => {
                resource
                    .try_mark_reconciliation_succeeded(
                        self.time_source.now(),
                        resource.metadata().generation,
                        success.stats,
                    )
                    .int_err()?;
            }
            Err(err) => {
                resource
                    .try_mark_reconciliation_failed(
                        self.time_source.now(),
                        resource.metadata().generation,
                        err.reason_code().to_string(),
                        err.to_string(),
                        SecretSetStats::default(),
                    )
                    .int_err()?;
            }
        }

        resource.save(self.event_store.as_ref()).await.int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
