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
    PendingStatusFromSpec,
    ResourceMetadata,
    ResourceSnapshot,
    ResourceStatusLike,
    ResourceUID,
    decode_typed_resource_snapshot,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceState<
    TSpec: std::fmt::Debug + Clone + Send + Sync,
    TStatus: ResourceStatusLike + std::fmt::Debug + Clone,
> {
    pub uid: ResourceUID,
    pub metadata: ResourceMetadata,
    pub spec: TSpec,
    pub status: TStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec, TStatus> ResourceState<TSpec, TStatus>
where
    TSpec: std::fmt::Debug + Clone + Send + Sync,
    TStatus: ResourceStatusLike + std::fmt::Debug + Clone,
{
    pub fn new(uid: ResourceUID, metadata: ResourceMetadata, spec: TSpec, status: TStatus) -> Self {
        Self {
            uid,
            metadata,
            spec,
            status,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec, TStatus> DeclarativeResourceState for ResourceState<TSpec, TStatus>
where
    TSpec: std::fmt::Debug + Clone + Send + Sync,
    TStatus: ResourceStatusLike + std::fmt::Debug + Clone,
{
    type Spec = TSpec;
    type Status = TStatus;

    fn uid(&self) -> &ResourceUID {
        &self.uid
    }

    fn metadata(&self) -> &ResourceMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut ResourceMetadata {
        &mut self.metadata
    }

    fn spec(&self) -> &Self::Spec {
        &self.spec
    }

    fn spec_mut(&mut self) -> &mut Self::Spec {
        &mut self.spec
    }

    fn status(&self) -> &Self::Status {
        &self.status
    }

    fn status_mut(&mut self) -> &mut Self::Status {
        &mut self.status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec, TStatus> TryFrom<ResourceSnapshot> for ResourceState<TSpec, TStatus>
where
    TSpec: std::fmt::Debug + Clone + Send + Sync + DeserializeOwned,
    TStatus: ResourceStatusLike
        + std::fmt::Debug
        + Clone
        + DeserializeOwned
        + PendingStatusFromSpec<TSpec>,
{
    type Error = InternalError;

    fn try_from(snapshot: ResourceSnapshot) -> Result<Self, Self::Error> {
        let (uid, metadata, spec, status) =
            decode_typed_resource_snapshot::<TSpec, TStatus>(snapshot)?;

        Ok(Self {
            uid,
            metadata,
            spec,
            status,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
