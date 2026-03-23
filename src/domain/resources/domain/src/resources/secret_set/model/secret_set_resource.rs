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
    ResourceID,
    ResourceMetadata,
    ResourceMetadataInput,
    ResourceStateFactory,
    ResourceValidateMetadata,
    ResourceValidateSpec,
    SecretSetEventStore,
    SecretSetLifecycleError,
    SecretSetSpec,
    SecretSetState,
    SecretSetStatus,
    try_update_resource_metadata,
    try_update_resource_spec,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SecretSetResource(Aggregate<SecretSetState, SecretSetEventStoreStatic>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type SecretSetEventStoreStatic = dyn SecretSetEventStore + 'static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetResource {
    pub fn try_create(
        now: DateTime<Utc>,
        resource_id: ResourceID,
        metadata: ResourceMetadataInput,
        spec: SecretSetSpec,
    ) -> Result<Self, SecretSetLifecycleError> {
        metadata.validate()?;
        spec.validate()?;

        use crate::ReconcilableResourceEventFactory;
        let event = Self::created_event(now, resource_id, metadata, spec);

        Aggregate::new(resource_id, event)
            .map(Self)
            .map_err(|e| SecretSetLifecycleError::InvariantViolation(Box::new(e)))
    }

    pub fn try_update_metadata(
        &mut self,
        now: DateTime<Utc>,
        new_metadata: ResourceMetadataInput,
    ) -> Result<(), SecretSetLifecycleError> {
        try_update_resource_metadata(self, now, new_metadata)
    }

    pub fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: SecretSetSpec,
    ) -> Result<(), SecretSetLifecycleError> {
        try_update_resource_spec(self, now, new_spec)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for SecretSetResource {
    type Spec = SecretSetSpec;
    type Status = SecretSetStatus;
    type ResourceState = SecretSetState;

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

impl ResourceStateFactory for SecretSetResource {
    fn state_from_created(
        resource_id: ResourceID,
        metadata: ResourceMetadata,
        spec: Self::Spec,
        status: Self::Status,
    ) -> Self::ResourceState {
        SecretSetState {
            resource_id,
            metadata,
            spec,
            status,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
