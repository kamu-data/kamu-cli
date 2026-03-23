// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::*;

use crate::{
    DeclarativeResource,
    ReconcilableEventSourcedResourceModel,
    ResourceID,
    ResourceMetadata,
    ResourceMetadataInput,
    ResourceValidateMetadata,
    ResourceValidateSpec,
    VariableSetEventStore,
    VariableSetFailureDetails,
    VariableSetLifecycleError,
    VariableSetReconcileSuccess,
    VariableSetSpec,
    VariableSetState,
    VariableSetStatus,
    VariableSetStatusProjector,
    try_update_resource_metadata,
    try_update_resource_spec,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct VariableSetResource(Aggregate<VariableSetState, VariableSetEventStoreStatic>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type VariableSetEventStoreStatic = dyn VariableSetEventStore + 'static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VariableSetResource {
    pub fn try_create(
        now: DateTime<Utc>,
        resource_id: ResourceID,
        metadata: ResourceMetadataInput,
        spec: VariableSetSpec,
    ) -> Result<Self, VariableSetLifecycleError> {
        metadata.validate()?;
        spec.validate()?;

        use crate::ReconcilableEventSourcedResource;
        let event = Self::make_created_event(now, resource_id, metadata, spec);

        Aggregate::new(resource_id, event)
            .map(Self)
            .map_err(|e| VariableSetLifecycleError::InvariantViolation(Box::new(e)))
    }

    pub fn try_update_metadata(
        &mut self,
        now: DateTime<Utc>,
        new_metadata: ResourceMetadataInput,
    ) -> Result<(), VariableSetLifecycleError> {
        try_update_resource_metadata(self, now, new_metadata)
    }

    pub fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: VariableSetSpec,
    ) -> Result<(), VariableSetLifecycleError> {
        try_update_resource_spec(self, now, new_spec)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for VariableSetResource {
    type Spec = VariableSetSpec;
    type Status = VariableSetStatus;
    type ResourceState = VariableSetState;

    fn resource_id(&self) -> &ResourceID {
        &self.as_ref().resource_id
    }

    fn metadata(&self) -> &ResourceMetadata {
        &self.as_ref().metadata
    }

    fn spec(&self) -> &Self::Spec {
        &self.as_ref().spec
    }

    fn status(&self) -> &Self::Status {
        &self.as_ref().status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VariableSetResourceModel {}

impl ReconcilableEventSourcedResourceModel for VariableSetResourceModel {
    type Spec = VariableSetSpec;
    type Status = VariableSetStatus;
    type Success = VariableSetReconcileSuccess;
    type FailureDetails = VariableSetFailureDetails;
    type State = VariableSetState;
    type StatusProjector = VariableSetStatusProjector;

    fn from_created(
        resource_id: ResourceID,
        metadata: ResourceMetadata,
        spec: Self::Spec,
        status: Self::Status,
    ) -> Self::State {
        VariableSetState {
            resource_id,
            metadata,
            spec,
            status,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
