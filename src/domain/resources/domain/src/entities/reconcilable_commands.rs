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
    ResourceMetadataInput,
    ResourceMetadataValidationError,
    ResourcePhase,
    ResourceValidateMetadata,
    ResourceValidateSpec,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait InvariantViolationOf<TState: Projection> {
    fn invariant_violation(error: ProjectionError<TState>) -> Self;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_create_reconcilable_resource<R, TCreated, TCreate>(
    now: DateTime<Utc>,
    resource_id: crate::ResourceID,
    metadata: ResourceMetadataInput,
    spec: R::Spec,
    create: TCreate,
) -> Result<TCreated, R::LifecycleError>
where
    R: ReconcilableEventSourcedResource,
    R::Spec: ResourceValidateSpec,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>
        + From<ResourceMetadataValidationError>
        + From<<R::Spec as ResourceValidateSpec>::ValidationError>,
    TCreate: FnOnce(
        crate::ResourceID,
        ReconcilableResourceEvent<R::Spec, R::ReconcileSuccess, R::FailureDetails>,
    ) -> Result<TCreated, ProjectionError<R::ResourceState>>,
{
    metadata.validate()?;
    spec.validate()?;

    let event = R::make_created_event(now, resource_id, metadata, spec);

    create(resource_id, event).map_err(R::LifecycleError::invariant_violation)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_update_resource_metadata<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    new_metadata: ResourceMetadataInput,
) -> Result<(), R::LifecycleError>
where
    R: ReconcilableEventSourcedResource,
    R::LifecycleError:
        From<ResourceMetadataValidationError> + InvariantViolationOf<R::ResourceState>,
{
    new_metadata.validate()?;

    if resource.metadata().is_equivalent_to(&new_metadata) {
        return Ok(());
    }

    let event = resource.make_metadata_updated_event(now, new_metadata);
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

    let event = resource.make_spec_updated_event(now, new_spec, resource.metadata().generation + 1);
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
    if resource.metadata().generation != expected_generation {
        tracing::warn!(
            expected_generation,
            current_generation = resource.metadata().generation,
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
    if resource.metadata().generation != expected_generation {
        tracing::warn!(
            expected_generation,
            current_generation = resource.metadata().generation,
            "Skipping stale reconciliation failure"
        );
        return Ok(());
    }

    let event = resource.make_reconciliation_failed_event(now, expected_generation, error);
    resource.apply_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
