// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{ConcurrentModificationError, LoadError, SaveError};
use internal_error::InternalError;

use crate::{DeclarativeResource, ReconcilableEventSourcedResource, ResourceID, ResourceSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourceParams<R: DeclarativeResource> {
    pub resource_id: Option<ResourceID>,
    pub metadata: crate::ResourceMetadataInput,
    pub spec: R::Spec,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyResourceOutcome {
    Created,
    Updated,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyResourceResult<R: DeclarativeResource> {
    pub resource_id: ResourceID,
    pub state: R::ResourceState,
    pub outcome: ApplyResourceOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ApplyResourceUseCase<R: ReconcilableEventSourcedResource>: Send + Sync {
    async fn execute(
        &self,
        params: ApplyResourceParams<R>,
    ) -> Result<ApplyResourceResult<R>, ApplyResourceUseCaseError<R>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ApplyResourceUseCaseError<R: ReconcilableEventSourcedResource> {
    #[error("Resource with the specified identity failed to load. Reason: {0}")]
    LoadFailed(LoadError<R::ResourceState>),

    #[error("Resource with id {0} was not found")]
    ResourceIdNotFound(ResourceID),

    #[error(
        "Resource id {resource_id} refers to {actual_kind}/{actual_api_version}, expected \
         {expected_kind}/{expected_api_version}"
    )]
    ResourceTypeMismatch {
        resource_id: ResourceID,
        expected_kind: String,
        expected_api_version: String,
        actual_kind: String,
        actual_api_version: String,
    },

    #[error(transparent)]
    Duplicate(crate::ResourceDuplicateError),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error("Resource with the specified identity failed to save. Reason: {0}")]
    SaveFailed(SaveError),

    #[error("Failed to synchronize resource snapshot")]
    SnapshotSyncFailed(#[source] InternalError),

    #[error(transparent)]
    Lifecycle(R::LifecycleError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> ApplyResourceUseCaseError<R>
where
    R: ReconcilableEventSourcedResource,
{
    pub fn type_mismatch(
        resource_id: ResourceID,
        expected: &crate::ResourceDescriptor,
        actual: &ResourceSnapshot,
    ) -> Self {
        Self::ResourceTypeMismatch {
            resource_id,
            expected_kind: expected.resource_type.to_string(),
            expected_api_version: expected.api_version.to_string(),
            actual_kind: actual.kind.clone(),
            actual_api_version: actual.api_version.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<InternalError> for ApplyResourceUseCaseError<R>
where
    R: ReconcilableEventSourcedResource,
{
    fn from(err: InternalError) -> Self {
        Self::SnapshotSyncFailed(err)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
