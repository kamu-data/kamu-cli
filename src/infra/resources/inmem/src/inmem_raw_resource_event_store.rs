// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use event_sourcing::{
    EventID,
    EventStore,
    EventStoreStateImpl,
    EventStream,
    GetEventsOpts,
    InMemoryEventStore,
    SaveEventsError,
};
use futures::TryStreamExt;
use internal_error::InternalError;
use kamu_resources::{
    ResourceRawEvent,
    ResourceRawEventProjection,
    ResourceRawEventQuery,
    ResourceRawEventStore,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryRawResourceEventStore {
    inner: InMemoryEventStore<
        ResourceRawEventProjection,
        EventStoreStateImpl<ResourceRawEventProjection>,
    >,
}

#[component(pub)]
#[interface(dyn ResourceRawEventStore)]
#[scope(Singleton)]
impl InMemoryRawResourceEventStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<ResourceRawEventProjection> for InMemoryRawResourceEventStore {
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        self.inner.total_events_stored().await
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, ResourceRawEvent> {
        Box::pin(
            self.inner
                .get_all_events(opts)
                .map_ok(|(event_id, mut event)| {
                    event.event_id = event_id;
                    (event_id, event)
                }),
        )
    }

    fn get_events(
        &self,
        query: &ResourceRawEventQuery,
        opts: GetEventsOpts,
    ) -> EventStream<'_, ResourceRawEvent> {
        Box::pin(
            self.inner
                .get_events(query, opts)
                .map_ok(|(event_id, mut event)| {
                    event.event_id = event_id;
                    (event_id, event)
                }),
        )
    }

    async fn save_events(
        &self,
        query: &ResourceRawEventQuery,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<ResourceRawEvent>,
    ) -> Result<EventID, SaveEventsError> {
        self.inner
            .save_events(query, maybe_prev_stored_event_id, events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceRawEventStore for InMemoryRawResourceEventStore {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
