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
    ResourceCondition,
    ResourceMetadata,
    ResourcePhase,
    ResourceState,
    VariableSetEvent,
    VariableSetID,
    VariableSetSpec,
    VariableSetStats,
    VariableSetStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VariableSetState = ResourceState<VariableSetSpec, VariableSetStatus>;

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
                    metadata: ResourceMetadata {
                        uid: e.variable_set_id,
                        name: e.name,
                        generation: 1,
                        created_at: e.event_time,
                        updated_at: e.event_time,
                        deleted_at: None,
                    },
                    spec: e.spec,
                    status: VariableSetStatus {
                        phase: ResourcePhase::Pending,
                        observed_generation: 0,
                        conditions: vec![],
                        stats: VariableSetStats {
                            total_variables: total,
                            valid_variables: 0,
                            invalid_variables: 0,
                        },
                    },
                })
            }

            (Some(mut s), E::SpecUpdated(e)) => {
                assert_eq!(s.metadata.uid, e.variable_set_id);

                let total = e.new_spec.variables.len();

                s.spec = e.new_spec;
                s.metadata.generation = e.new_generation;
                s.metadata.updated_at = e.event_time;

                s.status.phase = ResourcePhase::Pending;
                s.status.conditions = vec![];
                s.status.stats.total_variables = total;
                s.status.stats.valid_variables = 0;
                s.status.stats.invalid_variables = 0;

                Ok(s)
            }

            (Some(mut s), E::ReconciliationStarted(e)) => {
                assert_eq!(s.metadata.uid, e.variable_set_id);

                s.status.phase = ResourcePhase::Reconciling;
                ResourceCondition::set_condition(
                    &mut s.status.conditions,
                    ResourceCondition::reconciling_true(e.event_time),
                );

                Ok(s)
            }

            (Some(mut s), E::ReconciliationSucceeded(e)) => {
                assert_eq!(s.metadata.uid, e.variable_set_id);

                s.status.phase = ResourcePhase::Ready;
                s.status.observed_generation = e.generation;
                s.status.stats = e.stats;

                ResourceCondition::set_condition(
                    &mut s.status.conditions,
                    ResourceCondition::accepted_true(e.event_time),
                );
                ResourceCondition::set_condition(
                    &mut s.status.conditions,
                    ResourceCondition::ready_true(e.event_time),
                );
                ResourceCondition::set_condition(
                    &mut s.status.conditions,
                    ResourceCondition::reconciling_false(e.event_time),
                );

                Ok(s)
            }

            (Some(mut s), E::ReconciliationFailed(e)) => {
                assert_eq!(s.metadata.uid, e.variable_set_id);

                s.status.phase = ResourcePhase::Failed;
                s.status.observed_generation = e.generation;
                s.status.stats = e.stats;

                ResourceCondition::set_condition(
                    &mut s.status.conditions,
                    ResourceCondition::accepted_true(e.event_time),
                );
                ResourceCondition::set_condition(
                    &mut s.status.conditions,
                    ResourceCondition::ready_false(e.event_time, e.reason, e.message),
                );
                ResourceCondition::set_condition(
                    &mut s.status.conditions,
                    ResourceCondition::reconciling_false(e.event_time),
                );

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
