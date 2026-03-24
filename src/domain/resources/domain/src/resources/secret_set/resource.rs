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
use internal_error::InternalError;

use crate::{
    DeclarativeResource,
    ResourceApiVersion,
    ResourceID,
    ResourceMetadata,
    ResourceMetadataInput,
    ResourceType,
    SecretSetEventStore,
    SecretSetLifecycleError,
    SecretSetSpec,
    SecretSetState,
    SecretSetStatus,
    decode_typed_resource_snapshot,
    try_create_reconcilable_resource,
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
    pub const RESOURCE_TYPE: &'static str = "secret_set";
    pub const API_VERSION: &'static str = "v1alpha1";

    pub fn try_create(
        now: DateTime<Utc>,
        resource_id: ResourceID,
        metadata: ResourceMetadataInput,
        spec: SecretSetSpec,
    ) -> Result<Self, SecretSetLifecycleError> {
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

impl ResourceType for SecretSetResource {
    const RESOURCE_TYPE: &'static str = Self::RESOURCE_TYPE;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceApiVersion for SecretSetResource {
    const API_VERSION: &'static str = Self::API_VERSION;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for SecretSetResource {
    type Spec = SecretSetSpec;
    type Status = SecretSetStatus;
    type ResourceState = SecretSetState;

    fn decode_snapshot(
        snapshot: crate::ResourceSnapshot,
    ) -> Result<Self::ResourceState, InternalError> {
        let (resource_id, metadata, spec, status) =
            decode_typed_resource_snapshot::<SecretSetSpec, SecretSetStatus>(snapshot)?;

        Ok(SecretSetState {
            resource_id,
            metadata,
            spec,
            status,
        })
    }

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
