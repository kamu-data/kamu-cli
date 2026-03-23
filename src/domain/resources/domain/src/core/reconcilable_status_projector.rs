// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ProjectionError;

use crate::{
    DeclarativeResourceState,
    ReconcilableResourceEvent,
    ReconcilableResourceModel,
    ResourceMetadata,
    ResourceState,
    ResourceStatusLike,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait PendingStatusFromSpec<TSpec>: Sized {
    fn pending_from_spec(spec: &TSpec) -> Self;

    fn reset_pending_from_spec(&mut self, spec: &TSpec);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ReconcilableStatusProjector<TSpec, TSuccess, TFailureDetails> {
    type Status: PendingStatusFromSpec<TSpec>;

    fn new_pending(spec: &TSpec) -> Self::Status {
        Self::Status::pending_from_spec(spec)
    }

    fn on_spec_updated(status: &mut Self::Status, spec: &TSpec) {
        status.reset_pending_from_spec(spec);
    }

    fn on_reconciliation_succeeded(status: &mut Self::Status, success: TSuccess);

    fn on_reconciliation_failed(status: &mut Self::Status, details: TFailureDetails);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn project_reconcilable_resource_state<TModel>(
    state: Option<TModel::State>,
    event: ReconcilableResourceEvent<TModel::Spec, TModel::Success, TModel::FailureDetails>,
) -> Result<TModel::State, ProjectionError<TModel::State>>
where
    TModel: ReconcilableResourceModel,
{
    use ReconcilableResourceEvent as E;

    match (state, event) {
        (None, E::Created(e)) => {
            let pending_status = TModel::StatusProjector::new_pending(&e.spec);
            Ok(ResourceState::new(
                e.resource_id,
                ResourceMetadata::from_input(e.event_time, e.metadata),
                e.spec,
                pending_status,
            )
            .into())
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
            TModel::StatusProjector::on_spec_updated(s.status_mut(), &spec);

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
            TModel::StatusProjector::on_reconciliation_succeeded(s.status_mut(), e.success);

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
            TModel::StatusProjector::on_reconciliation_failed(s.status_mut(), e.details);

            Ok(s)
        }

        (state, event) => Err(ProjectionError::new(state, event)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
