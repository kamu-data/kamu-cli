// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{ResourceHeaders, ResourceID, ResourceSnapshot, ResourceStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DeclarativeResource:
    Sized + Send + Sync + std::fmt::Debug + AsRef<Self::ResourceState>
{
    type Spec: std::fmt::Debug + Send + Sync;
    type ResourceState: DeclarativeResourceState<Spec = Self::Spec>
        + TryFrom<ResourceSnapshot, Error = InternalError>
        + From<Self>;

    fn id(&self) -> &ResourceID {
        self.as_ref().id()
    }

    fn headers(&self) -> &ResourceHeaders {
        self.as_ref().headers()
    }

    fn spec(&self) -> &Self::Spec {
        self.as_ref().spec()
    }

    fn status(&self) -> &ResourceStatus {
        self.as_ref().status()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait DeclarativeResourceState: Send + Sync + std::fmt::Debug {
    type Spec: std::fmt::Debug + Send + Sync;

    fn id(&self) -> &ResourceID;

    fn headers(&self) -> &ResourceHeaders;
    fn headers_mut(&mut self) -> &mut ResourceHeaders;

    fn spec(&self) -> &Self::Spec;
    fn spec_mut(&mut self) -> &mut Self::Spec;

    fn status(&self) -> &ResourceStatus;
    fn status_mut(&mut self) -> &mut ResourceStatus;

    fn into_parts(self) -> (ResourceID, ResourceHeaders, Self::Spec, ResourceStatus)
    where
        Self: Sized;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
