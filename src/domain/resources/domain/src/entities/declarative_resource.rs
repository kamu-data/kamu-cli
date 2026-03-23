// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::Projection;

use crate::{ResourceID, ResourceMetadata, ResourceStatusLike};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DeclarativeResource: Send + Sync {
    type Spec: std::fmt::Debug + Send + Sync;
    type Status: ResourceStatusLike + std::fmt::Debug;
    type ResourceState: Projection
        + DeclarativeResourceState<Spec = Self::Spec, Status = Self::Status>;

    fn resource_id(&self) -> &ResourceID;
    fn metadata(&self) -> &ResourceMetadata;
    fn spec(&self) -> &Self::Spec;
    fn status(&self) -> &Self::Status;
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
