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
    SecretSetEvent,
    SecretSetID,
    SecretSetSpec,
    SecretSetStats,
    SecretSetStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type SecretSetState = ResourceState<SecretSetSpec, SecretSetStatus>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for SecretSetState {
    type Query = SecretSetID;
    type Event = SecretSetEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use SecretSetEvent as E;

        match (state, event) {
            (None, E::Created(e)) => {
                let total = e.spec.secrets.len();

                Ok(Self {
                    metadata: ResourceMetadata {
                        uid: e.secret_set_id,
                        name: e.name,
                        generation: 1,
                        created_at: e.event_time,
                        updated_at: e.event_time,
                        deleted_at: None,
                    },
                    spec: e.spec,
                    status: SecretSetStatus {
                        phase: ResourcePhase::Pending,
                        observed_generation: 0,
                        conditions: vec![],
                        stats: SecretSetStats {
                            total_secrets: total,
                            valid_secrets: 0,
                            invalid_secrets: 0,
                        },
                    },
                })
            }

            (Some(mut s), E::SpecUpdated(e)) => {
                assert_eq!(s.metadata.uid, e.secret_set_id);

                let total = e.new_spec.secrets.len();

                s.spec = e.new_spec;
                s.metadata.generation = e.new_generation;
                s.metadata.updated_at = e.event_time;

                s.status.phase = ResourcePhase::Pending;
                s.status.conditions.clear();
                s.status.stats = SecretSetStats {
                    total_secrets: total,
                    valid_secrets: 0,
                    invalid_secrets: 0,
                };

                Ok(s)
            }

            (Some(mut s), E::ReconciliationStarted(e)) => {
                assert_eq!(s.metadata.uid, e.secret_set_id);

                s.status.phase = ResourcePhase::Reconciling;
                ResourceCondition::set_condition(
                    &mut s.status.conditions,
                    ResourceCondition::reconciling_true(e.event_time),
                );

                Ok(s)
            }

            (Some(mut s), E::ReconciliationSucceeded(e)) => {
                assert_eq!(s.metadata.uid, e.secret_set_id);

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
                assert_eq!(s.metadata.uid, e.secret_set_id);

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

impl ProjectionEvent<SecretSetID> for SecretSetEvent {
    fn matches_query(&self, query: &SecretSetID) -> bool {
        self.secret_set_id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
