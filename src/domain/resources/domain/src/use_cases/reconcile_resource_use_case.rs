// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{LoadError, SaveError};

use crate::ReconcilableResource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ReconcileResourceUseCase<R: ReconcilableResource> {
    async fn execute(&self, id: &R::Identity) -> Result<(), ReconcileResourceUseCaseError<R>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ReconcileResourceUseCaseError<R: ReconcilableResource> {
    #[error("Resource with the specified identity failed to load. Reason: {0}")]
    LoadFailed(LoadError<R::ResourceState>),

    #[error("Resource with the specified identity failed to save. Reason: {0}")]
    SaveFailed(SaveError),

    #[error(transparent)]
    Lifecycle(R::LifecycleError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
