// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::{Projection, ProjectionError};

use crate::{
    ReconcilableEventSourcedResource,
    ReconcilableResourceEvent,
    ResourceHeadersInput,
    ResourceHeadersValidationError,
    ResourcePhase,
    ResourceValidateHeaders,
    ResourceValidateSpec,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait InvariantViolationOf<TState: Projection> {
    fn invariant_violation(error: ProjectionError<TState>) -> Self;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! impl_invariant_violation_lifecycle_error {
    ($lifecycle_error:ty, $state:ty) => {
        impl $crate::InvariantViolationOf<$state> for $lifecycle_error {
            fn invariant_violation(error: ::event_sourcing::ProjectionError<$state>) -> Self {
                Self::InvariantViolation(::std::boxed::Box::new(error))
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_create_reconcilable_resource<R, TCreated, TCreate>(
    now: DateTime<Utc>,
    uid: crate::ResourceUID,
    headers: ResourceHeadersInput,
    spec: R::Spec,
    create: TCreate,
) -> Result<TCreated, R::LifecycleError>
where
    R: ReconcilableEventSourcedResource,
    R::Spec: ResourceValidateSpec,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>
        + From<ResourceHeadersValidationError>
        + From<<R::Spec as ResourceValidateSpec>::ValidationError>,
    TCreate: FnOnce(
        crate::ResourceUID,
        ReconcilableResourceEvent<R::Spec, R::ReconcileSuccess, R::ReconcileFailureDetails>,
    ) -> Result<TCreated, ProjectionError<R::ResourceState>>,
{
    headers.validate()?;
    spec.validate()?;

    let event = R::make_created_event(now, uid, headers, spec);

    create(uid, event).map_err(R::LifecycleError::invariant_violation)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_update_resource_headers<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    new_headers: ResourceHeadersInput,
) -> Result<(), R::LifecycleError>
where
    R: ReconcilableEventSourcedResource,
    R::LifecycleError:
        From<ResourceHeadersValidationError> + InvariantViolationOf<R::ResourceState>,
{
    new_headers.validate()?;

    if resource.headers().is_equivalent_to(&new_headers) {
        return Ok(());
    }

    let event = resource.make_headers_updated_event(now, new_headers);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_update_resource_spec<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    new_spec: R::Spec,
) -> Result<(), R::LifecycleError>
where
    R: ReconcilableEventSourcedResource,
    R::Spec: ResourceValidateSpec + PartialEq + Clone,
    R::LifecycleError: From<<R::Spec as ResourceValidateSpec>::ValidationError>
        + InvariantViolationOf<R::ResourceState>,
{
    new_spec.validate()?;

    if resource.spec() == &new_spec {
        return Ok(());
    }

    let event = resource.make_spec_updated_event(now, new_spec, resource.headers().generation + 1);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_delete_resource<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    tombstone_name: String,
) -> Result<(), R::LifecycleError>
where
    R: ReconcilableEventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>,
{
    if resource.headers().deleted_at.is_some() {
        return Ok(());
    }

    let event = resource.make_deleted_event(now, tombstone_name);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_mark_resource_reconciliation_started<R>(
    resource: &mut R,
    now: DateTime<Utc>,
) -> Result<(), R::LifecycleError>
where
    R: ReconcilableEventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>,
{
    if !resource.needs_reconciliation() {
        return Ok(());
    }

    use crate::ResourceStatusLike;
    if resource.status().resource_status().phase == ResourcePhase::Reconciling {
        return Ok(());
    }

    let event = resource.make_reconciliation_started_event(now);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_mark_resource_reconciliation_succeeded<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    expected_generation: u64,
    success: R::ReconcileSuccess,
) -> Result<(), R::LifecycleError>
where
    R: ReconcilableEventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>,
{
    if resource.headers().generation != expected_generation {
        tracing::warn!(
            expected_generation,
            current_generation = resource.headers().generation,
            "Skipping stale reconciliation success"
        );
        return Ok(());
    }

    let event = resource.make_reconciliation_succeeded_event(now, expected_generation, success);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_mark_resource_reconciliation_failed<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    expected_generation: u64,
    error: &R::ReconcileError,
) -> Result<(), R::LifecycleError>
where
    R: ReconcilableEventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>,
{
    if resource.headers().generation != expected_generation {
        tracing::warn!(
            expected_generation,
            current_generation = resource.headers().generation,
            "Skipping stale reconciliation failure"
        );
        return Ok(());
    }

    let event = resource.make_reconciliation_failed_event(now, expected_generation, error);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
