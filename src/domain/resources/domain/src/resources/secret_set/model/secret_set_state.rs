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
    ReconcilableStatusProjector,
    ResourceID,
    ResourceState,
    SecretSetEvent,
    SecretSetFailureDetails,
    SecretSetReconcileSuccess,
    SecretSetResource,
    SecretSetSpec,
    SecretSetStats,
    SecretSetStatus,
    project_reconcilable_resource_state,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type SecretSetState = ResourceState<SecretSetSpec, SecretSetStatus>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
pub struct SecretSetStatusProjector;

impl ReconcilableStatusProjector<SecretSetSpec, SecretSetReconcileSuccess, SecretSetFailureDetails>
    for SecretSetStatusProjector
{
    type Status = SecretSetStatus;

    fn new_pending(spec: &SecretSetSpec) -> Self::Status {
        SecretSetStatus::new_pending(SecretSetStats {
            total_secrets: spec.secrets.len(),
            valid_secrets: 0,
            invalid_secrets: 0,
        })
    }

    fn on_spec_updated(status: &mut Self::Status, spec: &SecretSetSpec) {
        status.stats = SecretSetStats {
            total_secrets: spec.secrets.len(),
            valid_secrets: 0,
            invalid_secrets: 0,
        };
    }

    fn on_reconciliation_succeeded(status: &mut Self::Status, success: SecretSetReconcileSuccess) {
        status.stats = success.stats;
    }

    fn on_reconciliation_failed(status: &mut Self::Status, details: SecretSetFailureDetails) {
        status.stats = details.stats;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for SecretSetState {
    type Query = ResourceID;
    type Event = SecretSetEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        project_reconcilable_resource_state::<
            SecretSetResource,
            SecretSetSpec,
            SecretSetStatus,
            SecretSetReconcileSuccess,
            SecretSetFailureDetails,
            SecretSetStatusProjector,
        >(state, event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<ResourceID> for SecretSetEvent {
    fn matches_query(&self, query: &ResourceID) -> bool {
        self.resource_id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
