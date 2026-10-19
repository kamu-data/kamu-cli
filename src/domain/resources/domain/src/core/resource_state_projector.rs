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
    ResourceEvent,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceSpecFromInput,
    ResourceState,
    ResourceStateModel,
    ResourceStatusExt,
    new_pending_resource_status,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn project_resource_state<TModel>(
    state: Option<TModel::State>,
    event: ResourceEvent<TModel::SpecInput>,
) -> Result<TModel::State, ProjectionError<TModel::State>>
where
    TModel: ResourceStateModel,
    TModel::Spec: ResourceSpecFromInput<TModel::SpecInput>,
{
    use ResourceEvent as E;

    match (state, event) {
        (None, E::Created(e)) => Ok(ResourceState::new(
            e.id,
            ResourceHeaders::from_input(e.event_time, e.id, e.headers),
            TModel::Spec::from_input(e.spec),
            new_pending_resource_status(),
        )
        .into()),

        (Some(s), event) if s.headers().deleted_at.is_some() => {
            Err(ProjectionError::new(Some(s), event))
        }

        (Some(mut s), E::HeadersUpdated(e)) => {
            assert_eq!(s.id(), &e.id);

            s.headers_mut().apply_update(e.event_time, e.new_headers);
            if let Some(new_generation) = e.new_generation {
                s.headers_mut().generation = new_generation;
            }
            align_phase(&mut s);

            Ok(s)
        }

        (Some(mut s), E::SpecUpdated(e)) => {
            assert_eq!(s.id(), &e.id);

            *s.spec_mut() = TModel::Spec::from_input(e.new_spec);
            s.headers_mut().generation = e.new_generation;
            s.headers_mut().updated_at = e.event_time;
            align_phase(&mut s);

            Ok(s)
        }

        (Some(mut s), E::StatusUpdated(e)) => {
            assert_eq!(s.id(), &e.id);

            *s.status_mut() = e.new_status;
            s.headers_mut().status_generation += 1;
            s.headers_mut().updated_at = e.event_time;
            align_phase(&mut s);

            Ok(s)
        }

        (Some(mut s), E::Deleted(e)) => {
            assert_eq!(s.id(), &e.id);

            s.headers_mut().deleted_at = Some(e.event_time);
            s.headers_mut().updated_at = e.event_time;
            s.headers_mut().name = e.tombstone_name;

            Ok(s)
        }

        (state, event) => Err(ProjectionError::new(state, event)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn align_phase<TState: DeclarativeResourceState>(state: &mut TState) {
    let generation = state.headers().generation;
    state.status_mut().align_phase_with_generation(generation);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
