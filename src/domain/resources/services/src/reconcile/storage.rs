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
    ReconcilableResourceRepository,
    ReconcileResourceUseCase,
    ReconcileResourceUseCaseError,
    Reconciler,
    ResourceID,
    StorageResource,
};
use time_source::SystemTimeSource;

use crate::ReconcileResourceUseCaseHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ReconcileResourceUseCase<StorageResource>)]
pub struct StorageReconcileResourceUseCaseImpl {
    repo: Arc<dyn ReconcilableResourceRepository<StorageResource>>,
    reconciler: Arc<dyn Reconciler<StorageResource>>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ReconcileResourceUseCase<StorageResource> for StorageReconcileResourceUseCaseImpl {
    async fn execute(
        &self,
        id: &ResourceID,
    ) -> Result<(), ReconcileResourceUseCaseError<StorageResource>> {
        let helper = ReconcileResourceUseCaseHelper::new(
            self.repo.as_ref(),
            self.reconciler.as_ref(),
            self.time_source.as_ref(),
        );
        helper.execute_reconciliation(id).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
