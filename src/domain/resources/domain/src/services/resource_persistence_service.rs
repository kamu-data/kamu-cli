// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ConcurrentModificationError;
use internal_error::InternalError;

use crate::{EventSourcedResource, ResourceDuplicateError, ResourceDurableStateValidationError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourcePersistenceService<R: EventSourcedResource>: Send + Sync {
    async fn create(&self, resource: &mut R) -> Result<(), ResourcePersistenceError>;

    async fn save(&self, resource: &mut R) -> Result<(), ResourcePersistenceError>;

    /// Saves the resources in one batch; fails with a concurrent modification
    /// when any of them changed since it was loaded.
    async fn save_many(&self, resources: &mut [R]) -> Result<(), ResourcePersistenceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ResourcePersistenceError {
    #[error(transparent)]
    Duplicate(ResourceDuplicateError),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    InvalidDurableState(ResourceDurableStateValidationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
