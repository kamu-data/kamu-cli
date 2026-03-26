// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{Projection, ProjectionError};
use kamu_resources::{
    DeclarativeResourceState,
    ReconcilableStateModel,
    ResourceID,
    ResourceMetadata,
    ResourceState,
    project_reconcilable_resource_state,
};

use crate::{
    SecretSetEvent,
    SecretSetFailureDetails,
    SecretSetReconcileSuccess,
    SecretSetSpec,
    SecretSetStatus,
    SecretSetStatusProjector,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SecretSetState(pub ResourceState<SecretSetSpec, SecretSetStatus>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SecretSetStateModel {}

impl ReconcilableStateModel for SecretSetStateModel {
    type Spec = SecretSetSpec;
    type Status = SecretSetStatus;
    type Success = SecretSetReconcileSuccess;
    type FailureDetails = SecretSetFailureDetails;
    type State = SecretSetState;
    type StatusProjector = SecretSetStatusProjector;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for SecretSetState {
    type Query = ResourceID;
    type Event = SecretSetEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        project_reconcilable_resource_state::<SecretSetStateModel>(state, event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResourceState for SecretSetState {
    type Spec = SecretSetSpec;
    type Status = SecretSetStatus;

    fn resource_id(&self) -> &ResourceID {
        &self.0.resource_id
    }

    fn metadata(&self) -> &ResourceMetadata {
        &self.0.metadata
    }

    fn metadata_mut(&mut self) -> &mut ResourceMetadata {
        &mut self.0.metadata
    }

    fn spec(&self) -> &Self::Spec {
        &self.0.spec
    }

    fn spec_mut(&mut self) -> &mut Self::Spec {
        &mut self.0.spec
    }

    fn status(&self) -> &Self::Status {
        &self.0.status
    }

    fn status_mut(&mut self) -> &mut Self::Status {
        &mut self.0.status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<ResourceState<SecretSetSpec, SecretSetStatus>> for SecretSetState {
    fn from(value: ResourceState<SecretSetSpec, SecretSetStatus>) -> Self {
        Self(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
