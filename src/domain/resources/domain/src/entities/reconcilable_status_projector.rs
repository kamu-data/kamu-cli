// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{Projection, ProjectionError};

use crate::{
    DeclarativeResourceState,
    ReconcilableResourceEvent,
    ResourceMetadata,
    ResourceStateFactory,
    ResourceStatusLike,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ReconcilableStatusProjector<TSpec, TSuccess, TFailureDetails> {
    type Status;

    fn new_pending(spec: &TSpec) -> Self::Status;

    fn on_spec_updated(status: &mut Self::Status, spec: &TSpec);

    fn on_reconciliation_succeeded(status: &mut Self::Status, success: TSuccess);

    fn on_reconciliation_failed(status: &mut Self::Status, details: TFailureDetails);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn project_reconcilable_resource_state<
    TResource,
    TSpec,
    TStatus,
    TSuccess,
    TFailureDetails,
    TProjector,
>(
    state: Option<TResource::ResourceState>,
    event: ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>,
) -> Result<TResource::ResourceState, ProjectionError<TResource::ResourceState>>
where
    TResource: ResourceStateFactory<Spec = TSpec, Status = TStatus>,
    TResource::ResourceState:
        Projection<Event = ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>>,
    TSpec: std::fmt::Debug + Clone + Send + Sync,
    TProjector: ReconcilableStatusProjector<TSpec, TSuccess, TFailureDetails, Status = TStatus>,
    TStatus: ResourceStatusLike + std::fmt::Debug + Clone + Send + Sync,
{
    use ReconcilableResourceEvent as E;

    match (state, event) {
        (None, E::Created(e)) => {
            let pending_status = TProjector::new_pending(&e.spec);
            Ok(TResource::state_from_created(
                e.resource_id,
                ResourceMetadata::from_input(e.event_time, e.metadata),
                e.spec,
                pending_status,
            ))
        }

        (Some(mut s), E::MetadataUpdated(e)) => {
            assert_eq!(s.resource_id(), &e.resource_id);

            s.metadata_mut().apply_update(e.event_time, e.new_metadata);

            Ok(s)
        }

        (Some(mut s), E::SpecUpdated(e)) => {
            assert_eq!(s.resource_id(), &e.resource_id);

            *s.spec_mut() = e.new_spec;
            s.metadata_mut().generation = e.new_generation;
            s.metadata_mut().updated_at = e.event_time;

            s.status_mut()
                .resource_status_mut()
                .mark_pending_for_new_generation();

            let spec = s.spec().clone();
            TProjector::on_spec_updated(s.status_mut(), &spec);

            Ok(s)
        }

        (Some(mut s), E::ReconciliationStarted(e)) => {
            assert_eq!(s.resource_id(), &e.resource_id);

            s.status_mut()
                .resource_status_mut()
                .mark_reconciling(e.event_time);

            Ok(s)
        }

        (Some(mut s), E::ReconciliationSucceeded(e)) => {
            assert_eq!(s.resource_id(), &e.resource_id);

            s.status_mut()
                .resource_status_mut()
                .mark_ready(e.event_time, e.generation);
            TProjector::on_reconciliation_succeeded(s.status_mut(), e.success);

            Ok(s)
        }

        (Some(mut s), E::ReconciliationFailed(e)) => {
            assert_eq!(s.resource_id(), &e.resource_id);

            s.status_mut().resource_status_mut().mark_failed(
                e.event_time,
                e.generation,
                e.reason,
                e.message,
            );
            TProjector::on_reconciliation_failed(s.status_mut(), e.details);

            Ok(s)
        }

        (state, event) => Err(ProjectionError::new(state, event)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
