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
    ReconcilableStateModel,
    ResourceID,
    ResourceState,
    SecretSetEvent,
    SecretSetFailureDetails,
    SecretSetReconcileSuccess,
    SecretSetSpec,
    SecretSetStatus,
    SecretSetStatusProjector,
    project_reconcilable_resource_state,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type SecretSetState = ResourceState<SecretSetSpec, SecretSetStatus>;

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
