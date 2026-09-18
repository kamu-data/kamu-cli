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
    ReconcilableResourceEvent,
    ReconcilableStateModel,
    ResourceID,
    ResourceSnapshot,
    ResourceState,
    ResourceStatus,
    project_reconcilable_resource_state,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel,
{
    inner: ResourceState<TModel::Spec>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel,
{
    pub fn into_inner(self) -> ResourceState<TModel::Spec> {
        self.inner
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> std::fmt::Debug for ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<TModel> Clone for ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> std::ops::Deref for ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel,
{
    type Target = ResourceState<TModel::Spec>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<TModel> std::ops::DerefMut for ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> From<ResourceState<TModel::Spec>> for ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel,
{
    fn from(value: ResourceState<TModel::Spec>) -> Self {
        Self { inner: value }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> DeclarativeResourceState for ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel,
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

impl<TModel> Projection for ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel<State = Self> + 'static,
    TModel::Spec: crate::ResourceSpecFromInput<TModel::SpecInput>,
{
    type Query = ResourceID;
    type Event =
        ReconcilableResourceEvent<TModel::SpecInput, TModel::Success, TModel::FailureDetails>;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        project_reconcilable_resource_state::<TModel>(state, event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TModel> TryFrom<ResourceSnapshot> for ReconcilableResourceState<TModel>
where
    TModel: ReconcilableStateModel,
    ResourceState<TModel::Spec>: TryFrom<ResourceSnapshot, Error = InternalError>,
{
    type Error = InternalError;

    fn try_from(snapshot: ResourceSnapshot) -> Result<Self, Self::Error> {
        ResourceState::<TModel::Spec>::try_from(snapshot).map(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
