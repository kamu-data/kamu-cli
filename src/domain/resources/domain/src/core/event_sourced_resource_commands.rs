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
    EventSourcedResource,
    ResourceCleanupError,
    ResourceDeletionInProgressError,
    ResourceEvent,
    ResourceHeadersExt,
    ResourceHeadersInput,
    ResourceHeadersValidationError,
    ResourceName,
    ResourcePhase,
    ResourceReconcileError,
    ResourceSpecFromInput,
    ResourceStatus,
    ResourceStatusConflictError,
    ResourceStatusExt,
    ResourceValidateHeaders,
    ResourceValidateSpec,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait InvariantViolationOf<TState: Projection> {
    fn invariant_violation(error: ProjectionError<TState>) -> Self;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lifecycle errors of every resource type can carry a status conflict, so
/// generic code can tell it apart from other failures.
pub trait StatusConflictOf: From<ResourceStatusConflictError> {
    fn as_status_conflict(&self) -> Option<&ResourceStatusConflictError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implements [`InvariantViolationOf`] and [`StatusConflictOf`] for a lifecycle
/// error enum with `InvariantViolation` and `StatusConflict` variants.
#[macro_export]
macro_rules! impl_resource_lifecycle_error {
    ($lifecycle_error:ty, $state:ty) => {
        impl $crate::InvariantViolationOf<$state> for $lifecycle_error {
            fn invariant_violation(error: ::event_sourcing::ProjectionError<$state>) -> Self {
                Self::InvariantViolation(::std::boxed::Box::new(error))
            }
        }

        impl $crate::StatusConflictOf for $lifecycle_error {
            fn as_status_conflict(&self) -> Option<&$crate::ResourceStatusConflictError> {
                if let Self::StatusConflict(err) = self {
                    Some(err)
                } else {
                    None
                }
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_create_resource<R, TCreated, TCreate>(
    now: DateTime<Utc>,
    id: crate::ResourceID,
    headers: ResourceHeadersInput,
    spec: R::SpecInput,
    create: TCreate,
) -> Result<TCreated, R::LifecycleError>
where
    R: EventSourcedResource,
    R::SpecInput: ResourceValidateSpec,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>
        + From<ResourceHeadersValidationError>
        + From<<R::SpecInput as ResourceValidateSpec>::ValidationError>,
    TCreate: FnOnce(
        crate::ResourceID,
        ResourceEvent<R::SpecInput>,
    ) -> Result<TCreated, ProjectionError<R::ResourceState>>,
{
    headers.validate()?;
    spec.validate()?;

    let event = R::make_created_event(now, id, headers, spec);

    create(id, event).map_err(R::LifecycleError::invariant_violation)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_update_resource_headers<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    new_headers: ResourceHeadersInput,
    new_generation: u64,
) -> Result<(), R::LifecycleError>
where
    R: EventSourcedResource,
    R::LifecycleError:
        From<ResourceHeadersValidationError> + InvariantViolationOf<R::ResourceState>,
{
    ensure_not_being_deleted(resource)?;
    new_headers.validate()?;

    if resource.headers().is_equivalent_to(&new_headers) {
        return Ok(());
    }

    let event = resource.make_headers_updated_event(now, new_headers, new_generation);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_update_resource_spec<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    new_spec: R::SpecInput,
    new_generation: u64,
) -> Result<(), R::LifecycleError>
where
    R: EventSourcedResource,
    R::SpecInput: ResourceValidateSpec + Clone,
    R::Spec: ResourceSpecFromInput<R::SpecInput> + PartialEq,
    R::LifecycleError: From<<R::SpecInput as ResourceValidateSpec>::ValidationError>
        + InvariantViolationOf<R::ResourceState>,
{
    ensure_not_being_deleted(resource)?;
    new_spec.validate()?;

    // Compare after normalizing (not the raw input) so a resubmitted-but-equivalent
    // input is correctly detected as unchanged, even if its written form differs.
    if resource.spec() == &R::Spec::from_input(new_spec.clone()) {
        return Ok(());
    }

    let event = resource.make_spec_updated_event(now, new_spec, new_generation);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Starts the deletion: the resource keeps its name and waits for its
/// controllers to clean up. Requesting it again is not an update.
pub fn try_request_resource_deletion<R>(
    resource: &mut R,
    now: DateTime<Utc>,
) -> Result<(), R::LifecycleError>
where
    R: EventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>,
{
    if resource.headers().deletion_requested_at.is_some() {
        return Ok(());
    }

    let event = resource.make_deletion_requested_event(now);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Completes a requested deletion and renames the resource to its tombstone
/// name. Completing it again is not an update; completing a deletion that was
/// never requested is an invariant violation, since it would skip the cleanup.
pub fn try_complete_resource_deletion<R>(
    resource: &mut R,
    now: DateTime<Utc>,
) -> Result<(), R::LifecycleError>
where
    R: EventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>,
{
    if resource.headers().deleted_at.is_some() {
        return Ok(());
    }

    let tombstone_name = ResourceName::new_unchecked(&format!("deleted-{}", resource.id()));
    let event = resource.make_deleted_event(now, tombstone_name);

    if resource.headers().deletion_requested_at.is_none() {
        return Err(R::LifecycleError::invariant_violation(
            ProjectionError::new(Some(resource.aggregate().as_state().clone()), event),
        ));
    }

    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn ensure_not_being_deleted<R>(resource: &R) -> Result<(), ResourceDeletionInProgressError>
where
    R: EventSourcedResource,
{
    if resource.headers().deletion_requested_at.is_some() {
        return Err(ResourceDeletionInProgressError { id: *resource.id() });
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Replaces the status, provided it is still at `expected_status_generation`.
/// A status equal to the current one is not an update.
pub fn try_update_resource_status<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    expected_status_generation: u64,
    new_status: ResourceStatus,
) -> Result<(), R::LifecycleError>
where
    R: EventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState> + StatusConflictOf,
{
    ensure_status_generation(resource, expected_status_generation)?;

    if resource.status() == &new_status {
        return Ok(());
    }

    let event = resource.make_status_updated_event(now, new_status);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checked before a new status is derived from the current one, since a
/// transition derived from another writer's status may be invalid.
fn ensure_status_generation<R>(
    resource: &R,
    expected_status_generation: u64,
) -> Result<(), ResourceStatusConflictError>
where
    R: EventSourcedResource,
{
    let actual_status_generation = resource.headers().status_generation;
    if actual_status_generation != expected_status_generation {
        return Err(ResourceStatusConflictError {
            expected: expected_status_generation,
            actual: actual_status_generation,
        });
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_mark_resource_reconciliation_started<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    expected_status_generation: u64,
) -> Result<(), R::LifecycleError>
where
    R: EventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState> + StatusConflictOf,
{
    if !resource.needs_reconciliation() {
        return Ok(());
    }

    if resource.status().phase == ResourcePhase::Reconciling {
        return Ok(());
    }

    ensure_status_generation(resource, expected_status_generation)?;

    let mut new_status = resource.status().clone();
    new_status.mark_reconciling(resource.headers().generation, now);

    try_update_resource_status(resource, now, expected_status_generation, new_status)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_mark_resource_reconciliation_succeeded<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    expected_generation: u64,
    expected_status_generation: u64,
) -> Result<(), R::LifecycleError>
where
    R: EventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState> + StatusConflictOf,
{
    if resource.headers().generation != expected_generation {
        tracing::warn!(
            expected_generation,
            current_generation = resource.headers().generation,
            "Skipping stale reconciliation success"
        );
        return Ok(());
    }

    if resource.headers().deletion_requested_at.is_some() {
        tracing::warn!("Skipping reconciliation success of a resource being deleted");
        return Ok(());
    }

    ensure_status_generation(resource, expected_status_generation)?;

    let mut new_status = resource.status().clone();
    new_status.mark_ready(expected_generation, now);

    try_update_resource_status(resource, now, expected_status_generation, new_status)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_mark_resource_reconciliation_failed<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    expected_generation: u64,
    expected_status_generation: u64,
    error: &R::ReconcileError,
) -> Result<(), R::LifecycleError>
where
    R: EventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState> + StatusConflictOf,
{
    if resource.headers().generation != expected_generation {
        tracing::warn!(
            expected_generation,
            current_generation = resource.headers().generation,
            "Skipping stale reconciliation failure"
        );
        return Ok(());
    }

    if resource.headers().deletion_requested_at.is_some() {
        tracing::warn!("Skipping reconciliation failure of a resource being deleted");
        return Ok(());
    }

    ensure_status_generation(resource, expected_status_generation)?;

    let mut new_status = resource.status().clone();
    new_status.mark_failed_or_degraded(error.code(), error.user_message());

    try_update_resource_status(resource, now, expected_status_generation, new_status)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records a failed cleanup of a resource being deleted. The deletion stays
/// in progress until a cleanup succeeds.
pub fn try_mark_resource_cleanup_failed<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    expected_status_generation: u64,
    error: &ResourceCleanupError,
) -> Result<(), R::LifecycleError>
where
    R: EventSourcedResource,
    R::LifecycleError: InvariantViolationOf<R::ResourceState> + StatusConflictOf,
{
    if !resource.needs_cleanup() {
        return Ok(());
    }

    ensure_status_generation(resource, expected_status_generation)?;

    let mut new_status = resource.status().clone();
    new_status.mark_cleanup_failed(error.to_string());

    try_update_resource_status(resource, now, expected_status_generation, new_status)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
