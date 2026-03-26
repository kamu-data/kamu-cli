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

use crate::{StorageEventStore, StorageSpec, StorageState, StorageStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct StorageResource(pub(crate) Aggregate<StorageState, StorageEventStoreStatic>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type StorageEventStoreStatic = dyn StorageEventStore + 'static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageResource {
    pub const RESOURCE_TYPE: &'static str = "storage";
    pub const API_VERSION: &'static str = "v1alpha1";
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceType for StorageResource {
    const RESOURCE_TYPE: &'static str = Self::RESOURCE_TYPE;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceApiVersion for StorageResource {
    const API_VERSION: &'static str = Self::API_VERSION;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for StorageResource {
    type Spec = StorageSpec;
    type Status = StorageStatus;
    type ResourceState = StorageState;

    fn decode_snapshot(snapshot: ResourceSnapshot) -> Result<Self::ResourceState, InternalError> {
        let (resource_id, metadata, spec, status) =
            decode_typed_resource_snapshot::<StorageSpec, StorageStatus>(snapshot)?;

        Ok(StorageState(ResourceState {
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
