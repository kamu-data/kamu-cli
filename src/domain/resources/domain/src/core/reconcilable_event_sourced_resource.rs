// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::{AggregateAccess, Projection};
use internal_error::InternalError;
use serde::Serialize;

use crate::{
    DeclarativeResource,
    InvariantViolationOf,
    ReconcilableResource,
    ReconcilableResourceEvent,
    ResourceDescriptorProvider,
    ResourceEventCreated,
    ResourceEventDeleted,
    ResourceEventMetadataUpdated,
    ResourceEventReconciliationFailed,
    ResourceEventReconciliationStarted,
    ResourceEventReconciliationSucceeded,
    ResourceEventSpecUpdated,
    ResourceID,
    ResourceMetadataInput,
    ResourceReconcileError,
    ResourceSnapshot,
    ResourceStatusLike,
    make_typed_resource_snapshot,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ReconcilableEventSourcedResource:
    ReconcilableResource
    + DeclarativeResource<
        ResourceState: Projection<
            Event = ReconcilableResourceEvent<
                Self::Spec,
                Self::ReconcileSuccess,
                Self::FailureDetails,
            >,
        >,
    > + AggregateAccess<Projection = Self::ResourceState>
{
    fn apply_event(
        &mut self,
        event: ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails>,
    ) -> Result<(), Self::LifecycleError>
    where
        Self::LifecycleError: InvariantViolationOf<Self::ResourceState>,
    {
        self.aggregate_mut()
            .apply(event)
            .map_err(Self::LifecycleError::invariant_violation)
    }

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

    fn make_deleted_event(
        &self,
        now: DateTime<Utc>,
        tombstone_name: String,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails> {
        ReconcilableResourceEvent::Deleted(ResourceEventDeleted {
            event_time: now,
            resource_id: *self.resource_id(),
            tombstone_name,
        })
    }

    fn make_resource_snapshot(&self) -> Result<ResourceSnapshot, InternalError>
    where
        Self: ResourceDescriptorProvider,
        Self::Spec: Serialize,
        Self::Status: Serialize + ResourceStatusLike,
    {
        make_typed_resource_snapshot(
            *self.resource_id(),
            Self::DESCRIPTOR.resource_type,
            Self::DESCRIPTOR.api_version,
            self.metadata().clone(),
            self.spec(),
            self.status(),
            self.aggregate().last_stored_event_id(),
        )
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
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::FailureDetails> {
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
