// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{Projection, ProjectionError};
use internal_error::InternalError;

use crate::{
    DeclarativeResourceState,
    ResourceEvent,
    ResourceID,
    ResourceSnapshot,
    ResourceState,
    ResourceStateModel,
    ResourceStatus,
    project_resource_state,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel,
{
    inner: ResourceState<TModel::Spec>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel,
{
    pub fn into_inner(self) -> ResourceState<TModel::Spec> {
        self.inner
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> std::fmt::Debug for EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<TModel> Clone for EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> std::ops::Deref for EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel,
{
    type Target = ResourceState<TModel::Spec>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<TModel> std::ops::DerefMut for EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> From<ResourceState<TModel::Spec>> for EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel,
{
    fn from(value: ResourceState<TModel::Spec>) -> Self {
        Self { inner: value }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> DeclarativeResourceState for EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel,
{
    type Spec = TModel::Spec;

    fn id(&self) -> &ResourceID {
        &self.inner.id
    }

    fn headers(&self) -> &crate::ResourceHeaders {
        &self.inner.headers
    }

    fn headers_mut(&mut self) -> &mut crate::ResourceHeaders {
        &mut self.inner.headers
    }

    fn spec(&self) -> &Self::Spec {
        &self.inner.spec
    }

    fn spec_mut(&mut self) -> &mut Self::Spec {
        &mut self.inner.spec
    }

    fn status(&self) -> &ResourceStatus {
        &self.inner.status
    }

    fn status_mut(&mut self) -> &mut ResourceStatus {
        &mut self.inner.status
    }

    fn into_parts(
        self,
    ) -> (
        ResourceID,
        crate::ResourceHeaders,
        Self::Spec,
        ResourceStatus,
    ) {
        self.inner.into_parts()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> Projection for EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel<State = Self> + 'static,
    TModel::Spec: crate::ResourceSpecFromInput<TModel::SpecInput>,
{
    type Query = ResourceID;
    type Event = ResourceEvent<TModel::SpecInput>;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        project_resource_state::<TModel>(state, event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> TryFrom<ResourceSnapshot> for EventSourcedResourceState<TModel>
where
    TModel: ResourceStateModel,
    ResourceState<TModel::Spec>: TryFrom<ResourceSnapshot, Error = InternalError>,
{
    type Error = InternalError;

    fn try_from(snapshot: ResourceSnapshot) -> Result<Self, Self::Error> {
        ResourceState::<TModel::Spec>::try_from(snapshot).map(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
