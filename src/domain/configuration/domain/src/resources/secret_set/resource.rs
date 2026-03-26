// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::*;
use internal_error::InternalError;
use kamu_resources::{
    DeclarativeResource,
    ResourceApiVersion,
    ResourceID,
    ResourceMetadata,
    ResourceSnapshot,
    ResourceState,
    ResourceType,
    decode_typed_resource_snapshot,
};

use crate::{SecretSetEventStore, SecretSetSpec, SecretSetState, SecretSetStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SecretSetResource(pub(crate) Aggregate<SecretSetState, SecretSetEventStoreStatic>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type SecretSetEventStoreStatic = dyn SecretSetEventStore + 'static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetResource {
    pub const RESOURCE_TYPE: &'static str = "secret_set";
    pub const API_VERSION: &'static str = "v1alpha1";
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

    fn decode_snapshot(snapshot: ResourceSnapshot) -> Result<Self::ResourceState, InternalError> {
        let (resource_id, metadata, spec, status) =
            decode_typed_resource_snapshot::<SecretSetSpec, SecretSetStatus>(snapshot)?;

        Ok(SecretSetState(ResourceState {
            resource_id,
            metadata,
            spec,
            status,
        }))
    }

    fn into_state(self) -> Self::ResourceState {
        self.0.into_state()
    }

    fn resource_id(&self) -> &ResourceID {
        &self.as_ref().0.resource_id
    }

    fn metadata(&self) -> &ResourceMetadata {
        &self.as_ref().0.metadata
    }

    fn spec(&self) -> &Self::Spec {
        &self.as_ref().0.spec
    }

    fn status(&self) -> &Self::Status {
        &self.as_ref().0.status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
