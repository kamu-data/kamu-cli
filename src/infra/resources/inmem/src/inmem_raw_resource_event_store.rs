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
    MultiEventStream,
    SaveEventsError,
    SaveEventsItem,
};
use futures::{StreamExt, TryStreamExt, future};
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

    fn get_events_multi(
        &self,
        queries: &[ResourceRawEventQuery],
    ) -> MultiEventStream<'_, ResourceRawEventQuery, ResourceRawEvent> {
        Box::pin(
            self.inner
                .get_events_multi(queries)
                .map_ok(|(query, event_id, mut event)| {
                    event.event_id = event_id;
                    (query, event_id, event)
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

    async fn save_events_multi(
        &self,
        items: Vec<SaveEventsItem<ResourceRawEventQuery, ResourceRawEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        self.inner.save_events_multi(items).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRawEventStore for InMemoryRawResourceEventStore {
    async fn total_events_stored_by_kind(&self, kind: &str) -> Result<usize, InternalError> {
        let mut events = self.get_all_events(GetEventsOpts::default());
        let mut count = 0;

        while let Some(event) = events.next().await {
            let (_, event) = match event {
                Ok(event) => event,
                Err(err) => match err {
                    event_sourcing::GetEventsError::Internal(err) => return Err(err),
                },
            };

            if event.query.kind == kind {
                count += 1;
            }
        }

        Ok(count)
    }

    fn get_all_events_by_kind(
        &self,
        kind: &str,
        opts: GetEventsOpts,
    ) -> EventStream<'_, ResourceRawEvent> {
        let kind = kind.to_string();

        Box::pin(self.get_all_events(opts).filter_map(move |event| {
            let kind = kind.clone();

            future::ready(match event {
                Ok((event_id, event)) if event.query.kind == kind => Some(Ok((event_id, event))),
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            })
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
