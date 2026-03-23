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
    VariableSetEvent,
    VariableSetFailureDetails,
    VariableSetReconcileSuccess,
    VariableSetSpec,
    VariableSetStatus,
    VariableSetStatusProjector,
    project_reconcilable_resource_state,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VariableSetState = ResourceState<VariableSetSpec, VariableSetStatus>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VariableSetStateModel {}

impl ReconcilableStateModel for VariableSetStateModel {
    type Spec = VariableSetSpec;
    type Status = VariableSetStatus;
    type Success = VariableSetReconcileSuccess;
    type FailureDetails = VariableSetFailureDetails;
    type State = VariableSetState;
    type StatusProjector = VariableSetStatusProjector;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for VariableSetState {
    type Query = ResourceID;
    type Event = VariableSetEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        project_reconcilable_resource_state::<VariableSetStateModel>(state, event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
