// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::ConcurrentModificationError;
use internal_error::InternalError;

use crate::{ReconcilableEventSourcedResource, ResourceDuplicateError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourcePersistenceService<R: ReconcilableEventSourcedResource>: Send + Sync {
    async fn create(&self, resource: &mut R) -> Result<(), ResourcePersistenceError>;

    async fn save(&self, resource: &mut R) -> Result<(), ResourcePersistenceError>;

    async fn delete(
        &self,
        resource: &mut R,
        now: DateTime<Utc>,
    ) -> Result<(), ResourcePersistenceError>;

    async fn delete_many(
        &self,
        resources: &mut [R],
        now: DateTime<Utc>,
    ) -> Result<(), ResourcePersistenceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ResourcePersistenceError {
    #[error(transparent)]
    Duplicate(ResourceDuplicateError),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
