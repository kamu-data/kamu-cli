// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{ResourceID, ResourceMetadata, ResourceSnapshot, ResourceStatusLike};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DeclarativeResource: Sized + Send + Sync + AsRef<Self::ResourceState> {
    type Spec: std::fmt::Debug + Send + Sync;
    type Status: ResourceStatusLike + std::fmt::Debug;
    type ResourceState: DeclarativeResourceState<Spec = Self::Spec, Status = Self::Status>
        + From<Self>;

    fn decode_snapshot(snapshot: ResourceSnapshot) -> Result<Self::ResourceState, InternalError>;

    fn resource_id(&self) -> &ResourceID {
        self.as_ref().resource_id()
    }

    fn metadata(&self) -> &ResourceMetadata {
        self.as_ref().metadata()
    }

    fn spec(&self) -> &Self::Spec {
        self.as_ref().spec()
    }

    fn status(&self) -> &Self::Status {
        self.as_ref().status()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DeclarativeResourceState: Send + Sync {
    type Spec: std::fmt::Debug + Send + Sync;
    type Status: ResourceStatusLike + std::fmt::Debug;

    fn resource_id(&self) -> &ResourceID;

    fn metadata(&self) -> &ResourceMetadata;
    fn metadata_mut(&mut self) -> &mut ResourceMetadata;

    fn spec(&self) -> &Self::Spec;
    fn spec_mut(&mut self) -> &mut Self::Spec;

    fn status(&self) -> &Self::Status;
    fn status_mut(&mut self) -> &mut Self::Status;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
