// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{Projection, ProjectionError};

use crate::{
    ReconcilableResourceModel,
    ResourceID,
    ResourceState,
    StorageEvent,
    StorageFailureDetails,
    StorageReconcileSuccess,
    StorageSpec,
    StorageStatus,
    StorageStatusProjector,
    project_reconcilable_resource_state,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type StorageState = ResourceState<StorageSpec, StorageStatus>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct StorageResourceModel {}

impl ReconcilableResourceModel for StorageResourceModel {
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
        project_reconcilable_resource_state::<StorageResourceModel>(state, event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
