// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use serde::de::DeserializeOwned;

use crate::{
    DeclarativeResourceState,
    ResourceHeaders,
    ResourceID,
    ResourceSnapshot,
    ResourceStatus,
    decode_typed_resource_snapshot,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceState<TSpec: std::fmt::Debug + Clone + Send + Sync> {
    pub id: ResourceID,
    pub headers: ResourceHeaders,
    pub spec: TSpec,
    pub status: ResourceStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec> ResourceState<TSpec>
where
    TSpec: std::fmt::Debug + Clone + Send + Sync,
{
    pub fn new(
        id: ResourceID,
        headers: ResourceHeaders,
        spec: TSpec,
        status: ResourceStatus,
    ) -> Self {
        Self {
            id,
            headers,
            spec,
            status,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec> DeclarativeResourceState for ResourceState<TSpec>
where
    TSpec: std::fmt::Debug + Clone + Send + Sync,
{
    type Spec = TSpec;

    fn id(&self) -> &ResourceID {
        &self.id
    }

    fn headers(&self) -> &ResourceHeaders {
        &self.headers
    }

    fn headers_mut(&mut self) -> &mut ResourceHeaders {
        &mut self.headers
    }

    fn spec(&self) -> &Self::Spec {
        &self.spec
    }

    fn spec_mut(&mut self) -> &mut Self::Spec {
        &mut self.spec
    }

    fn status(&self) -> &ResourceStatus {
        &self.status
    }

    fn status_mut(&mut self) -> &mut ResourceStatus {
        &mut self.status
    }

    fn into_parts(self) -> (ResourceID, ResourceHeaders, Self::Spec, ResourceStatus) {
        (self.id, self.headers, self.spec, self.status)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec> TryFrom<ResourceSnapshot> for ResourceState<TSpec>
where
    TSpec: std::fmt::Debug + Clone + Send + Sync + DeserializeOwned,
{
    type Error = InternalError;

    fn try_from(snapshot: ResourceSnapshot) -> Result<Self, Self::Error> {
        let (id, headers, spec, status) = decode_typed_resource_snapshot::<TSpec>(snapshot)?;

        Ok(Self {
            id,
            headers,
            spec,
            status,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
