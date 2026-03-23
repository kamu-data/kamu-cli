// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{Projection, ProjectionError, ProjectionEvent};

use crate::{
    ReconcilableEventSourcedResourceModel,
    ReconcilableStatusProjector,
    ResourceID,
    ResourceState,
    VariableSetEvent,
    VariableSetFailureDetails,
    VariableSetReconcileSuccess,
    VariableSetSpec,
    VariableSetStatus,
    project_reconcilable_resource_state,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VariableSetState = ResourceState<VariableSetSpec, VariableSetStatus>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VariableSetResourceModel {}

impl ReconcilableEventSourcedResourceModel for VariableSetResourceModel {
    type Spec = VariableSetSpec;
    type Status = VariableSetStatus;
    type Success = VariableSetReconcileSuccess;
    type FailureDetails = VariableSetFailureDetails;
    type State = VariableSetState;
    type StatusProjector = VariableSetStatusProjector;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VariableSetStatusProjector;

impl
    ReconcilableStatusProjector<
        VariableSetSpec,
        VariableSetReconcileSuccess,
        VariableSetFailureDetails,
    > for VariableSetStatusProjector
{
    type Status = VariableSetStatus;

    fn on_reconciliation_succeeded(
        status: &mut Self::Status,
        success: VariableSetReconcileSuccess,
    ) {
        status.stats = success.stats;
    }

    fn on_reconciliation_failed(status: &mut Self::Status, details: VariableSetFailureDetails) {
        status.stats = details.stats;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for VariableSetState {
    type Query = ResourceID;
    type Event = VariableSetEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        project_reconcilable_resource_state::<VariableSetResourceModel>(state, event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<ResourceID> for VariableSetEvent {
    fn matches_query(&self, query: &ResourceID) -> bool {
        self.resource_id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
