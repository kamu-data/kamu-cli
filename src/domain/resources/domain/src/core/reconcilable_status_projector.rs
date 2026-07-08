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
    ReconcilableStateModel,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceState,
    ResourceStatusExt,
    new_pending_resource_status,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn project_reconcilable_resource_state<TModel>(
    state: Option<TModel::State>,
    event: ReconcilableResourceEvent<TModel::Spec, TModel::Success, TModel::FailureDetails>,
) -> Result<TModel::State, ProjectionError<TModel::State>>
where
    TModel: ReconcilableStateModel,
{
    use ReconcilableResourceEvent as E;

    match (state, event) {
        (None, E::Created(e)) => Ok(ResourceState::new(
            e.id,
            ResourceHeaders::from_input(e.event_time, e.id, e.headers),
            e.spec,
            new_pending_resource_status(),
        )
        .into()),

        (Some(s), event) if s.headers().deleted_at.is_some() => {
            Err(ProjectionError::new(Some(s), event))
        }

        (Some(mut s), E::HeadersUpdated(e)) => {
            assert_eq!(s.id(), &e.id);

            s.headers_mut().apply_update(e.event_time, e.new_headers);

            Ok(s)
        }

        (Some(mut s), E::SpecUpdated(e)) => {
            assert_eq!(s.id(), &e.id);

            *s.spec_mut() = e.new_spec;
            s.headers_mut().generation = e.new_generation;
            s.headers_mut().updated_at = e.event_time;

            s.status_mut().mark_pending_for_new_generation();

            Ok(s)
        }

        (Some(mut s), E::Deleted(e)) => {
            assert_eq!(s.id(), &e.id);

            s.headers_mut().deleted_at = Some(e.event_time);
            s.headers_mut().updated_at = e.event_time;
            s.headers_mut().name = e.tombstone_name;

            Ok(s)
        }

        (Some(mut s), E::ReconciliationStarted(e)) => {
            assert_eq!(s.id(), &e.id);

            s.status_mut().mark_reconciling(e.event_time);

            Ok(s)
        }

        (Some(mut s), E::ReconciliationSucceeded(e)) => {
            assert_eq!(s.id(), &e.id);

            s.status_mut().mark_ready(e.event_time, e.generation);

            Ok(s)
        }

        (Some(mut s), E::ReconciliationFailed(e)) => {
            assert_eq!(s.id(), &e.id);

            s.status_mut()
                .mark_failed(e.event_time, e.generation, e.reason, e.message);

            Ok(s)
        }

        (state, event) => Err(ProjectionError::new(state, event)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
