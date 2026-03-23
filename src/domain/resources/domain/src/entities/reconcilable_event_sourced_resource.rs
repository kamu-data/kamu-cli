// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::Projection;

use crate::{
    DeclarativeResource,
    ReconcilableResource,
    ReconcilableResourceEvent,
    ReconcileFailureMapper,
    ResourceEventCreated,
    ResourceEventMetadataUpdated,
    ResourceEventReconciliationFailed,
    ResourceEventReconciliationStarted,
    ResourceEventReconciliationSucceeded,
    ResourceEventSpecUpdated,
    ResourceID,
    ResourceMetadataInput,
    ResourceReconcileError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ReconcilableEventSourcedResource:
    ReconcilableResource + DeclarativeResource<ResourceState: Projection>
{
    fn apply_event(
        &mut self,
        event: ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails>,
    ) -> Result<(), Self::LifecycleError>;

    fn make_created_event(
        now: DateTime<Utc>,
        resource_id: ResourceID,
        metadata: ResourceMetadataInput,
        spec: Self::Spec,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails> {
        ReconcilableResourceEvent::Created(ResourceEventCreated {
            event_time: now,
            resource_id,
            metadata,
            spec,
        })
    }

    fn make_metadata_updated_event(
        &self,
        now: DateTime<Utc>,
        new_metadata: ResourceMetadataInput,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails> {
        ReconcilableResourceEvent::MetadataUpdated(ResourceEventMetadataUpdated {
            event_time: now,
            resource_id: *self.resource_id(),
            new_metadata,
        })
    }

    fn make_spec_updated_event(
        &self,
        now: DateTime<Utc>,
        new_spec: Self::Spec,
        new_generation: u64,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails> {
        ReconcilableResourceEvent::SpecUpdated(ResourceEventSpecUpdated {
            event_time: now,
            resource_id: *self.resource_id(),
            new_spec,
            new_generation,
        })
    }

    fn make_reconciliation_started_event(
        &self,
        now: DateTime<Utc>,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails> {
        ReconcilableResourceEvent::ReconciliationStarted(ResourceEventReconciliationStarted {
            event_time: now,
            resource_id: *self.resource_id(),
            generation: self.metadata().generation,
        })
    }

    fn make_reconciliation_succeeded_event(
        &self,
        now: DateTime<Utc>,
        expected_generation: u64,
        success: Self::ReconcileSuccess,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails> {
        ReconcilableResourceEvent::ReconciliationSucceeded(ResourceEventReconciliationSucceeded {
            event_time: now,
            resource_id: *self.resource_id(),
            generation: expected_generation,
            success,
        })
    }

    fn make_reconciliation_failed_event(
        &self,
        now: DateTime<Utc>,
        expected_generation: u64,
        error: &Self::ReconcileError,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails>
    where
        Self: ReconcileFailureMapper,
    {
        ReconcilableResourceEvent::ReconciliationFailed(ResourceEventReconciliationFailed {
            event_time: now,
            resource_id: *self.resource_id(),
            generation: expected_generation,
            reason: error.reason_code().to_string(),
            message: error.user_message(),
            details: Self::failure_details(error),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
