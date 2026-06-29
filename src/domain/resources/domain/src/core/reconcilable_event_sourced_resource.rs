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
    ReconcilableResource,
    ReconcilableResourceEvent,
    ResourceDescriptorProvider,
    ResourceEventCreated,
    ResourceEventDeleted,
    ResourceEventHeadersUpdated,
    ResourceEventReconciliationFailed,
    ResourceEventReconciliationStarted,
    ResourceEventReconciliationSucceeded,
    ResourceEventSpecUpdated,
    ResourceHeadersInput,
    ResourceReconcileError,
    ResourceSnapshot,
    ResourceStatusLike,
    ResourceUID,
    make_typed_resource_snapshot,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ReconcilableEventSourcedResource:
    ReconcilableResource
    + DeclarativeResource<
        ResourceState: Projection<
            Query = ResourceUID,
            Event = ReconcilableResourceEvent<
                Self::Spec,
                Self::ReconcileSuccess,
                Self::ReconcileFailureDetails,
            >,
        >,
    > + AggregateAccess<Projection = Self::ResourceState>
{
    fn apply_event(
        &mut self,
        event: ReconcilableResourceEvent<
            Self::Spec,
            Self::ReconcileSuccess,
            Self::ReconcileFailureDetails,
        >,
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
        uid: ResourceUID,
        headers: ResourceHeadersInput,
        spec: Self::Spec,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::ReconcileFailureDetails>
    {
        ReconcilableResourceEvent::Created(ResourceEventCreated {
            event_time: now,
            uid,
            headers,
            spec,
        })
    }

    fn make_headers_updated_event(
        &self,
        now: DateTime<Utc>,
        new_headers: ResourceHeadersInput,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::ReconcileFailureDetails>
    {
        ReconcilableResourceEvent::HeadersUpdated(ResourceEventHeadersUpdated {
            event_time: now,
            uid: *self.uid(),
            new_headers,
        })
    }

    fn make_spec_updated_event(
        &self,
        now: DateTime<Utc>,
        new_spec: Self::Spec,
        new_generation: u64,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::ReconcileFailureDetails>
    {
        ReconcilableResourceEvent::SpecUpdated(ResourceEventSpecUpdated {
            event_time: now,
            uid: *self.uid(),
            new_spec,
            new_generation,
        })
    }

    fn make_deleted_event(
        &self,
        now: DateTime<Utc>,
        tombstone_name: String,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::ReconcileFailureDetails>
    {
        ReconcilableResourceEvent::Deleted(ResourceEventDeleted {
            event_time: now,
            uid: *self.uid(),
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
            *self.uid(),
            Self::DESCRIPTOR.resource_type,
            Self::DESCRIPTOR.api_version,
            self.headers().clone(),
            self.spec(),
            self.status(),
            self.aggregate().last_stored_event_id(),
        )
    }

    fn make_reconciliation_started_event(
        &self,
        now: DateTime<Utc>,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::ReconcileFailureDetails>
    {
        ReconcilableResourceEvent::ReconciliationStarted(ResourceEventReconciliationStarted {
            event_time: now,
            uid: *self.uid(),
            generation: self.headers().generation,
        })
    }

    fn make_reconciliation_succeeded_event(
        &self,
        now: DateTime<Utc>,
        expected_generation: u64,
        success: Self::ReconcileSuccess,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::ReconcileFailureDetails>
    {
        ReconcilableResourceEvent::ReconciliationSucceeded(ResourceEventReconciliationSucceeded {
            event_time: now,
            uid: *self.uid(),
            generation: expected_generation,
            success,
        })
    }

    fn make_reconciliation_failed_event(
        &self,
        now: DateTime<Utc>,
        expected_generation: u64,
        error: &Self::ReconcileError,
    ) -> ReconcilableResourceEvent<Self::Spec, Self::ReconcileSuccess, Self::ReconcileFailureDetails>
    {
        ReconcilableResourceEvent::ReconciliationFailed(ResourceEventReconciliationFailed {
            event_time: now,
            uid: *self.uid(),
            generation: expected_generation,
            reason: error.reason_code().to_string(),
            message: error.user_message(),
            details: Self::reconcile_failure_details(error),
        })
    }

    fn try_create(
        now: DateTime<Utc>,
        uid: ResourceUID,
        headers: ResourceHeadersInput,
        spec: Self::Spec,
    ) -> Result<Self, Self::LifecycleError>
    where
        Self: Sized,
        Self::Spec: crate::ResourceValidateSpec,
        Self::LifecycleError: InvariantViolationOf<Self::ResourceState>
            + From<crate::ResourceHeadersValidationError>
            + From<<Self::Spec as crate::ResourceValidateSpec>::ValidationError>,
    {
        crate::try_create_reconcilable_resource::<Self, _, _>(
            now,
            uid,
            headers,
            spec,
            Aggregate::new,
        )
        .map(Self::from_aggregate)
    }

    fn try_update_headers(
        &mut self,
        now: DateTime<Utc>,
        new_headers: ResourceHeadersInput,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized,
        Self::LifecycleError:
            From<crate::ResourceHeadersValidationError> + InvariantViolationOf<Self::ResourceState>,
    {
        crate::try_update_resource_headers(self, now, new_headers)
    }

    fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: Self::Spec,
    ) -> Result<(), Self::LifecycleError>
    where
        Self: Sized,
        Self::Spec: crate::ResourceValidateSpec + PartialEq + Clone,
        Self::LifecycleError: From<<Self::Spec as crate::ResourceValidateSpec>::ValidationError>
            + InvariantViolationOf<Self::ResourceState>,
    {
        crate::try_update_resource_spec(self, now, new_spec)
    }

    fn from_aggregate(aggregate: Aggregate<Self::ResourceState, Self::Store>) -> Self
    where
        Self: Sized;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! impl_reconcilable_event_sourced_resource {
    (
        resource = $resource:ty,
        reconcile_success = $reconcile_success:ty,
        reconcile_error = $reconcile_error:ty,
        reconcile_failure_details = $reconcile_failure_details:ty,
        lifecycle_error = $lifecycle_error:ty,
        reconcile_failure_details_fn = |$error:ident| $body:block
    ) => {
        impl $crate::ReconcilableResource for $resource {
            type ReconcileSuccess = $reconcile_success;
            type ReconcileError = $reconcile_error;
            type ReconcileFailureDetails = $reconcile_failure_details;
            type LifecycleError = $lifecycle_error;

            fn try_create(
                now: ::chrono::DateTime<::chrono::Utc>,
                uid: $crate::ResourceUID,
                headers: $crate::ResourceHeadersInput,
                spec: Self::Spec,
            ) -> Result<Self, Self::LifecycleError> {
                <$resource as $crate::ReconcilableEventSourcedResource>::try_create(
                    now,
                    uid,
                    headers,
                    spec,
                )
            }

            fn try_update_headers(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                new_headers: $crate::ResourceHeadersInput,
            ) -> Result<(), Self::LifecycleError> {
                <$resource as $crate::ReconcilableEventSourcedResource>::try_update_headers(
                    self,
                    now,
                    new_headers,
                )
            }

            fn try_update_spec(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                new_spec: Self::Spec,
            ) -> Result<(), Self::LifecycleError> {
                <$resource as $crate::ReconcilableEventSourcedResource>::try_update_spec(
                    self,
                    now,
                    new_spec,
                )
            }

            fn try_delete(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                tombstone_name: String,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_delete_resource(self, now, tombstone_name)
            }

            fn try_mark_reconciliation_started(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_mark_resource_reconciliation_started(self, now)
            }

            fn try_mark_reconciliation_succeeded(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                expected_generation: u64,
                success: Self::ReconcileSuccess,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_mark_resource_reconciliation_succeeded(
                    self,
                    now,
                    expected_generation,
                    success,
                )
            }

            fn try_mark_reconciliation_failed(
                &mut self,
                now: ::chrono::DateTime<::chrono::Utc>,
                expected_generation: u64,
                error: &Self::ReconcileError,
            ) -> Result<(), Self::LifecycleError> {
                $crate::try_mark_resource_reconciliation_failed(
                    self,
                    now,
                    expected_generation,
                    error,
                )
            }

            fn reconcile_failure_details($error: &Self::ReconcileError) -> Self::ReconcileFailureDetails $body
        }

        impl $crate::ReconcilableEventSourcedResource for $resource {
            fn from_aggregate(
                aggregate: ::event_sourcing::Aggregate<Self::ResourceState, Self::Store>,
            ) -> Self {
                Self(aggregate)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
