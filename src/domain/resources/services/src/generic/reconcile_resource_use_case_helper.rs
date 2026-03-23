// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ReconcilableResource,
    ReconcilableResourceRepository,
    ReconcileResourceUseCaseError,
    Reconciler,
    ResourceID,
};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReconcileResourceUseCaseHelper<'a, R: ReconcilableResource> {
    repo: &'a dyn ReconcilableResourceRepository<R>,
    reconciler: &'a dyn Reconciler<R>,
    time_source: &'a dyn SystemTimeSource,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<'a, R: ReconcilableResource> ReconcileResourceUseCaseHelper<'a, R> {
    pub fn new(
        repo: &'a dyn ReconcilableResourceRepository<R>,
        reconciler: &'a dyn Reconciler<R>,
        time_source: &'a dyn SystemTimeSource,
    ) -> Self {
        Self {
            repo,
            reconciler,
            time_source,
        }
    }

    pub async fn execute_reconciliation(
        &self,
        id: &ResourceID,
    ) -> Result<(), ReconcileResourceUseCaseError<R>> {
        let mut resource = self
            .repo
            .load(id)
            .await
            .map_err(ReconcileResourceUseCaseError::LoadFailed)?;
        if !resource.needs_reconciliation() {
            return Ok(());
        }

        resource
            .try_mark_reconciliation_started(self.time_source.now())
            .map_err(ReconcileResourceUseCaseError::Lifecycle)?;

        self.repo
            .save(&mut resource)
            .await
            .map_err(ReconcileResourceUseCaseError::SaveFailed)?;

        match self.reconciler.reconcile(&resource).await {
            Ok(success) => {
                resource
                    .try_mark_reconciliation_succeeded(
                        self.time_source.now(),
                        resource.metadata().generation,
                        success,
                    )
                    .map_err(ReconcileResourceUseCaseError::Lifecycle)?;
            }
            Err(err) => {
                resource
                    .try_mark_reconciliation_failed(
                        self.time_source.now(),
                        resource.metadata().generation,
                        &err,
                    )
                    .map_err(ReconcileResourceUseCaseError::Lifecycle)?;
            }
        }

        self.repo
            .save(&mut resource)
            .await
            .map_err(ReconcileResourceUseCaseError::SaveFailed)?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
