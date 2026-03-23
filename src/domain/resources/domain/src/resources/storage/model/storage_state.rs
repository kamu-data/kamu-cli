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
    StorageEvent,
    StorageFailureDetails,
    StorageReconcileSuccess,
    StorageReferenceStatus,
    StorageResource,
    StorageSpec,
    StorageStatus,
    project_reconcilable_resource_state,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type StorageState = ResourceState<StorageSpec, StorageStatus>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct StorageStatusProjector;

impl ReconcilableStatusProjector<StorageSpec, StorageReconcileSuccess, StorageFailureDetails>
    for StorageStatusProjector
{
    type Status = StorageStatus;

    fn new_pending(spec: &StorageSpec) -> Self::Status {
        StorageStatus::new_pending(spec.provider.kind())
    }

    fn on_spec_updated(status: &mut Self::Status, spec: &StorageSpec) {
        status.provider_kind = spec.provider.kind();
        status.references = StorageReferenceStatus::default();
    }

    fn on_reconciliation_succeeded(status: &mut Self::Status, success: StorageReconcileSuccess) {
        status.provider_kind = success.provider_kind;
        status.references = success.references;
    }

    fn on_reconciliation_failed(status: &mut Self::Status, details: StorageFailureDetails) {
        status.references = details.references;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for StorageState {
    type Query = ResourceID;
    type Event = StorageEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        project_reconcilable_resource_state::<
            StorageResource,
            StorageSpec,
            StorageStatus,
            StorageReconcileSuccess,
            StorageFailureDetails,
            StorageStatusProjector,
        >(state, event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<ResourceID> for StorageEvent {
    fn matches_query(&self, query: &ResourceID) -> bool {
        self.resource_id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
