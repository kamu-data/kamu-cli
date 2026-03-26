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
    StorageEvent,
    StorageFailureDetails,
    StorageReconcileSuccess,
    StorageSpec,
    StorageStatus,
    StorageStatusProjector,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct StorageState(pub ResourceState<StorageSpec, StorageStatus>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct StorageStateModel {}

impl ReconcilableStateModel for StorageStateModel {
    type Spec = StorageSpec;
    type Status = StorageStatus;
    type Success = StorageReconcileSuccess;
    type FailureDetails = StorageFailureDetails;
    type State = StorageState;
    type StatusProjector = StorageStatusProjector;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for StorageState {
    type Query = ResourceID;
    type Event = StorageEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        project_reconcilable_resource_state::<StorageStateModel>(state, event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResourceState for StorageState {
    type Spec = StorageSpec;
    type Status = StorageStatus;

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

impl From<ResourceState<StorageSpec, StorageStatus>> for StorageState {
    fn from(value: ResourceState<StorageSpec, StorageStatus>) -> Self {
        Self(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
