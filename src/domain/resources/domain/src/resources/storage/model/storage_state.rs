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
    StorageEvent,
    StorageReferenceStatus,
    StorageSpec,
    StorageStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type StorageID = uuid::Uuid;

pub type StorageState = ResourceState<StorageID, StorageSpec, StorageStatus>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for StorageState {
    type Query = StorageID;
    type Event = StorageEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use StorageEvent as E;

        match (state, event) {
            (None, E::Created(e)) => Ok(Self {
                id: e.storage_id,
                metadata: ResourceMetadata::from_input(e.event_time, e.metadata),
                status: StorageStatus::new_pending(e.spec.provider.kind()),
                spec: e.spec,
            }),

            (Some(mut s), E::MetadataUpdated(e)) => {
                assert_eq!(s.id, e.storage_id);

                s.metadata.apply_update(e.event_time, e.new_metadata);

                Ok(s)
            }

            (Some(mut s), E::SpecUpdated(e)) => {
                assert_eq!(s.id, e.storage_id);

                s.spec = e.new_spec;
                s.metadata.generation = e.new_generation;
                s.metadata.updated_at = e.event_time;

                s.status.resource_status.mark_pending_for_new_generation();
                s.status.provider_kind = s.spec.provider.kind();
                s.status.references = StorageReferenceStatus::default();

                Ok(s)
            }

            (Some(mut s), E::ReconciliationStarted(e)) => {
                assert_eq!(s.id, e.storage_id);

                s.status.resource_status.mark_reconciling(e.event_time);

                Ok(s)
            }

            (Some(mut s), E::ReconciliationSucceeded(e)) => {
                assert_eq!(s.id, e.storage_id);

                s.status
                    .resource_status
                    .mark_ready(e.event_time, e.generation);
                s.status.provider_kind = e.success.provider_kind;
                s.status.references = e.success.references;

                Ok(s)
            }

            (Some(mut s), E::ReconciliationFailed(e)) => {
                assert_eq!(s.id, e.storage_id);

                s.status.resource_status.mark_failed(
                    e.event_time,
                    e.generation,
                    e.reason,
                    e.message,
                );
                s.status.references = e.references;

                Ok(s)
            }

            (state, event) => Err(ProjectionError::new(state, event)),
        }
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<StorageID> for StorageEvent {
    fn matches_query(&self, query: &StorageID) -> bool {
        self.storage_id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
