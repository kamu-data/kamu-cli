// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_resources::{
    DeclarativeResource,
    ReconcilableResource,
    ReconcileResourceUseCase,
    ReconcileResourceUseCaseError,
    Reconciler,
    ResourceReconcileError,
    VariableSetEventStore,
    VariableSetID,
    VariableSetResource,
    VariableSetStats,
};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VariableSetReconcileResourceUseCaseImpl {
    event_store: Arc<dyn VariableSetEventStore>,
    reconciler: Arc<dyn Reconciler<VariableSetResource>>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ReconcileResourceUseCase<VariableSetResource> for VariableSetReconcileResourceUseCaseImpl {
    async fn execute(
        &self,
        id: &VariableSetID,
    ) -> Result<(), ReconcileResourceUseCaseError<VariableSetResource>> {
        let mut resource = VariableSetResource::load(id, self.event_store.as_ref())
            .await
            .map_err(ReconcileResourceUseCaseError::LoadFailed)?;
        if !resource.needs_reconciliation() {
            return Ok(());
        }

        resource
            .try_mark_reconciliation_started(self.time_source.now())
            .map_err(ReconcileResourceUseCaseError::Lifecycle)?;

        resource
            .save(self.event_store.as_ref())
            .await
            .map_err(ReconcileResourceUseCaseError::SaveFailed)?;

        match self.reconciler.reconcile(&resource).await {
            Ok(success) => {
                resource
                    .try_mark_reconciliation_succeeded(
                        self.time_source.now(),
                        resource.metadata().generation,
                        success.stats,
                    )
                    .map_err(ReconcileResourceUseCaseError::Lifecycle)?;
            }
            Err(err) => {
                resource
                    .try_mark_reconciliation_failed(
                        self.time_source.now(),
                        resource.metadata().generation,
                        err.reason_code().to_string(),
                        err.to_string(),
                        VariableSetStats::default(),
                    )
                    .map_err(ReconcileResourceUseCaseError::Lifecycle)?;
            }
        }

        resource
            .save(self.event_store.as_ref())
            .await
            .map_err(ReconcileResourceUseCaseError::SaveFailed)?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
