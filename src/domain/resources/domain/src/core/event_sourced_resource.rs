// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::{Aggregate, AggregateAccess, Projection};
use internal_error::InternalError;
use serde::Serialize;

use crate::{
    DeclarativeResource,
    InvariantViolationOf,
    ManagedResource,
    ResourceEvent,
    ResourceEventCreated,
    ResourceEventDeleted,
    ResourceEventDeletionRequested,
    ResourceEventHeadersUpdated,
    ResourceEventSpecUpdated,
    ResourceEventStatusUpdated,
    ResourceHeadersInput,
    ResourceID,
    ResourceName,
    ResourceSchemaProvider,
    ResourceSnapshot,
    ResourceStatus,
    make_typed_resource_snapshot,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait EventSourcedResource:
    ManagedResource
    + DeclarativeResource<
        ResourceState: Projection<Query = ResourceID, Event = ResourceEvent<Self::SpecInput>>,
    > + AggregateAccess<Projection = Self::ResourceState>
{
    fn apply_event(
        &mut self,
        event: ResourceEvent<Self::SpecInput>,
    ) -> Result<(), Self::LifecycleError>
    where
        Self::LifecycleError: InvariantViolationOf<Self::ResourceState>,
    {
        self.aggregate_mut()
            .apply(event)
            .map_err(Self::LifecycleError::invariant_violation)
    }

    fn revert(&mut self) {
        self.aggregate_mut().revert();
    }

    fn make_created_event(
        now: DateTime<Utc>,
        id: ResourceID,
        headers: ResourceHeadersInput,
        spec: Self::SpecInput,
    ) -> ResourceEvent<Self::SpecInput> {
        ResourceEvent::Created(ResourceEventCreated {
            event_time: now,
            id,
            headers,
            spec,
        })
    }

    fn make_headers_updated_event(
        &self,
        now: DateTime<Utc>,
        new_headers: ResourceHeadersInput,
        new_generation: u64,
    ) -> ResourceEvent<Self::SpecInput> {
        ResourceEvent::HeadersUpdated(ResourceEventHeadersUpdated {
            event_time: now,
            id: *self.id(),
            new_headers,
            new_generation: Some(new_generation),
        })
    }

    fn make_spec_updated_event(
        &self,
        now: DateTime<Utc>,
        new_spec: Self::SpecInput,
        new_generation: u64,
    ) -> ResourceEvent<Self::SpecInput> {
        ResourceEvent::SpecUpdated(ResourceEventSpecUpdated {
            event_time: now,
            id: *self.id(),
            new_spec,
            new_generation,
        })
    }

    fn make_deletion_requested_event(&self, now: DateTime<Utc>) -> ResourceEvent<Self::SpecInput> {
        ResourceEvent::DeletionRequested(ResourceEventDeletionRequested {
            event_time: now,
            id: *self.id(),
        })
    }

    fn make_deleted_event(
        &self,
        now: DateTime<Utc>,
        tombstone_name: ResourceName,
    ) -> ResourceEvent<Self::SpecInput> {
        ResourceEvent::Deleted(ResourceEventDeleted {
            event_time: now,
            id: *self.id(),
            tombstone_name,
        })
    }

    fn make_resource_snapshot(&self) -> Result<ResourceSnapshot, InternalError>
    where
        Self: ResourceSchemaProvider,
        Self::Spec: Serialize,
    {
        make_typed_resource_snapshot(
            *self.id(),
            Self::schema(),
            self.headers().clone(),
            self.spec(),
            self.status(),
            self.aggregate().last_stored_event_id(),
        )
    }

    fn make_status_updated_event(
        &self,
        now: DateTime<Utc>,
        new_status: ResourceStatus,
    ) -> ResourceEvent<Self::SpecInput> {
        ResourceEvent::StatusUpdated(ResourceEventStatusUpdated {
            event_time: now,
            id: *self.id(),
            new_status,
        })
    }

    fn try_create(
        now: DateTime<Utc>,
        id: ResourceID,
        headers: ResourceHeadersInput,
        spec: Self::SpecInput,
    ) -> Result<Self, Self::LifecycleError>
    where
        Self: Sized,
        Self::SpecInput: crate::ResourceValidateSpec,
        Self::LifecycleError: InvariantViolationOf<Self::ResourceState>
            + From<crate::ResourceHeadersValidationError>
            + From<<Self::SpecInput as crate::ResourceValidateSpec>::ValidationError>,
    {
        crate::try_create_resource::<Self, _, _>(now, id, headers, spec, Aggregate::new)
            .map(Self::from_aggregate)
    }

    fn try_update_headers(
        &mut self,
        now: DateTime<Utc>,
        new_headers: ResourceHeadersInput,
        new_generation: u64,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized,
        Self::LifecycleError:
            From<crate::ResourceHeadersValidationError> + InvariantViolationOf<Self::ResourceState>,
    {
        crate::try_update_resource_headers(self, now, new_headers, new_generation)
    }

    fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: Self::SpecInput,
        new_generation: u64,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized,
        Self::SpecInput: crate::ResourceValidateSpec + Clone,
        Self::Spec: crate::ResourceSpecFromInput<Self::SpecInput> + PartialEq,
        Self::LifecycleError: From<<Self::SpecInput as crate::ResourceValidateSpec>::ValidationError>
            + InvariantViolationOf<Self::ResourceState>,
    {
        crate::try_update_resource_spec(self, now, new_spec, new_generation)
    }

    fn from_aggregate(aggregate: Aggregate<Self::ResourceState, Self::Store>) -> Self
    where
        Self: Sized;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! impl_event_sourced_resource {
    (
        resource = $resource:ty,
        reconcile_error = $reconcile_error:ty,
        lifecycle_error = $lifecycle_error:ty $(,)?
    ) => {
        impl $crate::ManagedResource for $resource {
            type ReconcileError = $reconcile_error;
            type LifecycleError = $lifecycle_error;

            fn try_create(
                now: ::chrono::DateTime<::chrono::Utc>,
                id: $crate::ResourceID,
                headers: $crate::ResourceHeadersInput,
                spec: Self::SpecInput,
            ) -> Result<Self, Self::LifecycleError> {
                <$resource as $crate::EventSourcedResource>::try_create(now, id, headers, spec)
            }

            fn try_update_headers(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                new_headers: $crate::ResourceHeadersInput,
                new_generation: u64,
            ) -> Result<(), Self::LifecycleError> {
                <$resource as $crate::EventSourcedResource>::try_update_headers(
                    self,
                    now,
                    new_headers,
                    new_generation,
                )
            }

            fn try_update_spec(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                new_spec: Self::SpecInput,
                new_generation: u64,
            ) -> Result<(), Self::LifecycleError> {
                <$resource as $crate::EventSourcedResource>::try_update_spec(
                    self,
                    now,
                    new_spec,
                    new_generation,
                )
            }

            fn try_request_deletion(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_request_resource_deletion(self, now)
            }

            fn try_complete_deletion(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_complete_resource_deletion(self, now)
            }

            fn try_update_status(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                expected_status_generation: u64,
                new_status: $crate::ResourceStatus,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_update_resource_status(
                    self,
                    now,
                    expected_status_generation,
                    new_status,
                )
            }

            fn try_mark_reconciliation_started(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                expected_status_generation: u64,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_mark_resource_reconciliation_started(
                    self,
                    now,
                    expected_status_generation,
                )
            }

            fn try_mark_reconciliation_succeeded(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                expected_generation: u64,
                expected_status_generation: u64,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_mark_resource_reconciliation_succeeded(
                    self,
                    now,
                    expected_generation,
                    expected_status_generation,
                )
            }

            fn try_mark_reconciliation_failed(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                expected_generation: u64,
                expected_status_generation: u64,
                error: &Self::ReconcileError,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_mark_resource_reconciliation_failed(
                    self,
                    now,
                    expected_generation,
                    expected_status_generation,
                    error,
                )
            }

            fn try_mark_cleanup_failed(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                expected_status_generation: u64,
                error: &$crate::ResourceCleanupError,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_mark_resource_cleanup_failed(
                    self,
                    now,
                    expected_status_generation,
                    error,
                )
            }
        }

        impl $crate::EventSourcedResource for $resource {
            fn from_aggregate(
                aggregate: ::event_sourcing::Aggregate<Self::ResourceState, Self::Store>,
            ) -> Self {
                Self(aggregate)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
