// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{
    AppliesTypedEvent,
    ReconcilableResourceEventFactory,
    ResourceMetadataInput,
    ResourceMetadataValidationError,
    ResourcePhase,
    ResourceValidateMetadata,
    ResourceValidateSpec,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_update_resource_metadata<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    new_metadata: ResourceMetadataInput,
) -> Result<(), <R as AppliesTypedEvent<R::Event>>::LifecycleError>
where
    R: ReconcilableResourceEventFactory + AppliesTypedEvent<R::Event>,
    <R as AppliesTypedEvent<R::Event>>::LifecycleError: From<ResourceMetadataValidationError>,
{
    new_metadata.validate()?;

    if resource.metadata().is_equivalent_to(&new_metadata) {
        return Ok(());
    }

    let event = resource.metadata_updated_event(now, new_metadata);
    resource.apply_typed_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_update_resource_spec<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    new_spec: R::Spec,
) -> Result<(), <R as AppliesTypedEvent<R::Event>>::LifecycleError>
where
    R: ReconcilableResourceEventFactory + AppliesTypedEvent<R::Event>,
    R::Spec: ResourceValidateSpec + PartialEq + Clone,
    <R as AppliesTypedEvent<R::Event>>::LifecycleError:
        From<<R::Spec as ResourceValidateSpec>::ValidationError>,
{
    new_spec.validate()?;

    if resource.spec() == &new_spec {
        return Ok(());
    }

    let event = resource.spec_updated_event(now, new_spec, resource.metadata().generation + 1);
    resource.apply_typed_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_mark_resource_reconciliation_started<R>(
    resource: &mut R,
    now: DateTime<Utc>,
) -> Result<(), <R as AppliesTypedEvent<R::Event>>::LifecycleError>
where
    R: ReconcilableResourceEventFactory + AppliesTypedEvent<R::Event>,
{
    if !resource.needs_reconciliation() {
        return Ok(());
    }

    use crate::ResourceStatusLike;
    if resource.status().resource_status().phase == ResourcePhase::Reconciling {
        return Ok(());
    }

    let event = resource.reconciliation_started_event(now);
    resource.apply_typed_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_mark_resource_reconciliation_succeeded<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    expected_generation: u64,
    success: R::ReconcileSuccess,
) -> Result<(), <R as AppliesTypedEvent<R::Event>>::LifecycleError>
where
    R: ReconcilableResourceEventFactory + AppliesTypedEvent<R::Event>,
{
    if resource.metadata().generation != expected_generation {
        tracing::warn!(
            expected_generation,
            current_generation = resource.metadata().generation,
            "Skipping stale reconciliation success"
        );
        return Ok(());
    }

    let event = resource.reconciliation_succeeded_event(now, expected_generation, success);
    resource.apply_typed_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn try_mark_resource_reconciliation_failed<R>(
    resource: &mut R,
    now: DateTime<Utc>,
    expected_generation: u64,
    error: &R::ReconcileError,
) -> Result<(), <R as AppliesTypedEvent<R::Event>>::LifecycleError>
where
    R: ReconcilableResourceEventFactory + AppliesTypedEvent<R::Event>,
{
    if resource.metadata().generation != expected_generation {
        tracing::warn!(
            expected_generation,
            current_generation = resource.metadata().generation,
            "Skipping stale reconciliation failure"
        );
        return Ok(());
    }

    let event = resource.reconciliation_failed_event(now, expected_generation, error);
    resource.apply_typed_event(event)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
