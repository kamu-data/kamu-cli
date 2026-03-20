// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{Projection, ProjectionError, ProjectionEvent};

use crate::{
    ResourceMetadata,
    ResourceState,
    VariableSetEvent,
    VariableSetSpec,
    VariableSetStats,
    VariableSetStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VariableSetID = uuid::Uuid;

pub type VariableSetState = ResourceState<VariableSetID, VariableSetSpec, VariableSetStatus>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for VariableSetState {
    type Query = VariableSetID;
    type Event = VariableSetEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use VariableSetEvent as E;

        match (state, event) {
            (None, E::Created(e)) => {
                let total = e.spec.variables.len();

                Ok(Self {
                    id: e.variable_set_id,
                    metadata: ResourceMetadata::from_input(e.event_time, e.metadata),
                    spec: e.spec,
                    status: VariableSetStatus::new_pending(VariableSetStats {
                        total_variables: total,
                        valid_variables: 0,
                        invalid_variables: 0,
                    }),
                })
            }

            (Some(mut s), E::MetadataUpdated(e)) => {
                assert_eq!(s.id, e.variable_set_id);

                s.metadata.apply_update(e.event_time, e.new_metadata);

                Ok(s)
            }

            (Some(mut s), E::SpecUpdated(e)) => {
                assert_eq!(s.id, e.variable_set_id);

                let total = e.new_spec.variables.len();

                s.spec = e.new_spec;
                s.metadata.generation = e.new_generation;
                s.metadata.updated_at = e.event_time;

                s.status.resource_status.mark_pending_for_new_generation();
                s.status.stats = VariableSetStats {
                    total_variables: total,
                    valid_variables: 0,
                    invalid_variables: 0,
                };

                Ok(s)
            }

            (Some(mut s), E::ReconciliationStarted(e)) => {
                assert_eq!(s.id, e.variable_set_id);

                s.status.resource_status.mark_reconciling(e.event_time);

                Ok(s)
            }

            (Some(mut s), E::ReconciliationSucceeded(e)) => {
                assert_eq!(s.id, e.variable_set_id);

                s.status
                    .resource_status
                    .mark_ready(e.event_time, e.generation);
                s.status.stats = e.stats;

                Ok(s)
            }

            (Some(mut s), E::ReconciliationFailed(e)) => {
                assert_eq!(s.id, e.variable_set_id);

                s.status.resource_status.mark_failed(
                    e.event_time,
                    e.generation,
                    e.reason,
                    e.message,
                );
                s.status.stats = e.stats;

                Ok(s)
            }

            (state, event) => Err(ProjectionError::new(state, event)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<VariableSetID> for VariableSetEvent {
    fn matches_query(&self, query: &VariableSetID) -> bool {
        self.variable_set_id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
