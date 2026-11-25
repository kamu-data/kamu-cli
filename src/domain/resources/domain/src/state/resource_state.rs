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

    pub fn into_parts(self) -> (ResourceID, ResourceHeaders, TSpec, ResourceStatus) {
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
