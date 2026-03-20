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
    SecretSetEvent,
    SecretSetSpec,
    SecretSetStats,
    SecretSetStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type SecretSetID = uuid::Uuid;

pub type SecretSetState = ResourceState<SecretSetID, SecretSetSpec, SecretSetStatus>;

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
                    id: e.secret_set_id,
                    metadata: ResourceMetadata::from(e.metadata),
                    spec: e.spec,
                    status: SecretSetStatus::new_pending(SecretSetStats {
                        total_secrets: total,
                        valid_secrets: 0,
                        invalid_secrets: 0,
                    }),
                })
            }

            (Some(mut s), E::MetadataUpdated(e)) => {
                assert_eq!(s.id, e.secret_set_id);

                s.metadata.update(e.new_metadata);

                Ok(s)
            }

            (Some(mut s), E::SpecUpdated(e)) => {
                assert_eq!(s.id, e.secret_set_id);

                let total = e.new_spec.secrets.len();

                s.spec = e.new_spec;
                s.metadata.generation = e.new_generation;
                s.metadata.updated_at = e.event_time;

                s.status.resource_status.reset_for_new_spec();
                s.status.stats = SecretSetStats {
                    total_secrets: total,
                    valid_secrets: 0,
                    invalid_secrets: 0,
                };

                Ok(s)
            }

            (Some(mut s), E::ReconciliationStarted(e)) => {
                assert_eq!(s.id, e.secret_set_id);

                s.status.resource_status.mark_reconciling(e.event_time);

                Ok(s)
            }

            (Some(mut s), E::ReconciliationSucceeded(e)) => {
                assert_eq!(s.id, e.secret_set_id);

                s.status
                    .resource_status
                    .mark_ready(e.event_time, e.generation);
                s.status.stats = e.stats;

                Ok(s)
            }

            (Some(mut s), E::ReconciliationFailed(e)) => {
                assert_eq!(s.id, e.secret_set_id);

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

impl ProjectionEvent<SecretSetID> for SecretSetEvent {
    fn matches_query(&self, query: &SecretSetID) -> bool {
        self.secret_set_id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
