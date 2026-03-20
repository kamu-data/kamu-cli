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
    SecretSetID,
    SecretSetResource,
};
use time_source::SystemTimeSource;

use crate::ReconcileResourceUseCaseHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ReconcileResourceUseCase<SecretSetResource>)]
pub struct SecretSetReconcileResourceUseCaseImpl {
    repo: Arc<dyn ReconcilableResourceRepository<SecretSetResource>>,
    reconciler: Arc<dyn Reconciler<SecretSetResource>>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ReconcileResourceUseCase<SecretSetResource> for SecretSetReconcileResourceUseCaseImpl {
    async fn execute(
        &self,
        id: &SecretSetID,
    ) -> Result<(), ReconcileResourceUseCaseError<SecretSetResource>> {
        let helper = ReconcileResourceUseCaseHelper::new(
            self.repo.as_ref(),
            self.reconciler.as_ref(),
            self.time_source.as_ref(),
        );
        helper.execute_reconciliation(id).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
