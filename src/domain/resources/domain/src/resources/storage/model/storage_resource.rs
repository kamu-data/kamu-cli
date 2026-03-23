// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::*;

use crate::{
    DeclarativeResource,
    ReconcilableEventSourcedResourceModel,
    ResourceID,
    ResourceMetadata,
    ResourceMetadataInput,
    StorageEventStore,
    StorageFailureDetails,
    StorageLifecycleError,
    StorageReconcileSuccess,
    StorageSpec,
    StorageState,
    StorageStatus,
    StorageStatusProjector,
    try_create_reconcilable_resource,
    try_update_resource_metadata,
    try_update_resource_spec,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct StorageResource(Aggregate<StorageState, StorageEventStoreStatic>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type StorageEventStoreStatic = dyn StorageEventStore + 'static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageResource {
    pub fn try_create(
        now: DateTime<Utc>,
        resource_id: ResourceID,
        metadata: ResourceMetadataInput,
        spec: StorageSpec,
    ) -> Result<Self, StorageLifecycleError> {
        try_create_reconcilable_resource::<Self, _, _>(
            now,
            resource_id,
            metadata,
            spec,
            Aggregate::new,
        )
        .map(Self)
    }

    pub fn try_update_metadata(
        &mut self,
        now: DateTime<Utc>,
        new_metadata: ResourceMetadataInput,
    ) -> Result<(), StorageLifecycleError> {
        try_update_resource_metadata(self, now, new_metadata)
    }

    pub fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: StorageSpec,
    ) -> Result<(), StorageLifecycleError> {
        try_update_resource_spec(self, now, new_spec)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for StorageResource {
    type Spec = StorageSpec;
    type Status = StorageStatus;
    type ResourceState = StorageState;

    fn resource_id(&self) -> &ResourceID {
        &self.as_ref().resource_id
    }

    fn metadata(&self) -> &ResourceMetadata {
        &self.as_ref().metadata
    }

    fn spec(&self) -> &Self::Spec {
        &self.as_ref().spec
    }

    fn status(&self) -> &Self::Status {
        &self.as_ref().status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct StorageResourceModel {}

impl ReconcilableEventSourcedResourceModel for StorageResourceModel {
    type Spec = StorageSpec;
    type Status = StorageStatus;
    type Success = StorageReconcileSuccess;
    type FailureDetails = StorageFailureDetails;
    type State = StorageState;
    type StatusProjector = StorageStatusProjector;

    fn from_created(
        resource_id: ResourceID,
        metadata: ResourceMetadata,
        spec: Self::Spec,
        status: Self::Status,
    ) -> Self::State {
        StorageState::new(resource_id, metadata, spec, status)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
