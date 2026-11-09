// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{TransactionId, TransactionRef};
use dill::*;
use event_sourcing::{
    EventID,
    EventStore,
    EventStream,
    GetEventsOpts,
    MultiEventStream,
    SaveEventsError,
    SaveEventsItem,
};
use internal_error::InternalError;
use kamu_resources::{
    ResourceRawEvent,
    ResourceRawEventProjection,
    ResourceRawEventQuery,
    ResourceRawEventStore,
};

use crate::InMemoryRawResourceEventState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryRawResourceEventStore {
    state: Arc<InMemoryRawResourceEventState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn ResourceRawEventStore)]
impl InMemoryRawResourceEventStore {
    pub fn new(
        state: Arc<InMemoryRawResourceEventState>,
        transaction_ref: Option<TransactionRef>,
    ) -> Self {
        Self {
            state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
        }
    }

    fn stream_events(
        &self,
        opts: &GetEventsOpts,
        matches: impl Fn(&ResourceRawEvent) -> bool,
    ) -> EventStream<'_, ResourceRawEvent> {
        let events = self.state.events(self.transaction_id, opts, matches);
        Box::pin(futures::stream::iter(
            events.into_iter().map(|event| Ok((event.event_id, event))),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<ResourceRawEventProjection> for InMemoryRawResourceEventStore {
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        Ok(self.state.events_count(self.transaction_id, |_| true))
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, ResourceRawEvent> {
        self.stream_events(&opts, |_| true)
    }

    fn get_events(
        &self,
        query: &ResourceRawEventQuery,
        opts: GetEventsOpts,
    ) -> EventStream<'_, ResourceRawEvent> {
        let query = query.clone();
        self.stream_events(&opts, move |event| event.query == query)
    }

    fn get_events_multi(
        &self,
        queries: &[ResourceRawEventQuery],
    ) -> MultiEventStream<'_, ResourceRawEventQuery, ResourceRawEvent> {
        let events = self
            .state
            .events(self.transaction_id, &GetEventsOpts::default(), |event| {
                queries.contains(&event.query)
            });
        Box::pin(futures::stream::iter(
            events
                .into_iter()
                .map(|event| Ok((event.query.clone(), event.event_id, event))),
        ))
    }

    async fn save_events(
        &self,
        query: &ResourceRawEventQuery,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<ResourceRawEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut last_event_ids = self.state.save(
            self.transaction_id,
            vec![SaveEventsItem {
                query: query.clone(),
                maybe_prev_stored_event_id,
                events,
            }],
        )?;
        Ok(last_event_ids.remove(0))
    }

    async fn save_events_multi(
        &self,
        items: Vec<SaveEventsItem<ResourceRawEventQuery, ResourceRawEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        if items.is_empty() {
            return Ok(vec![]);
        }

        event_sourcing::validate_multi_save_items(&items)?;

        self.state.save(self.transaction_id, items)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRawEventStore for InMemoryRawResourceEventStore {
    async fn total_events_stored_by_schema(&self, schema: &str) -> Result<usize, InternalError> {
        Ok(self.state.events_count(self.transaction_id, |event| {
            event.query.schema.as_str() == schema
        }))
    }

    fn get_all_events_by_schema(
        &self,
        schema: &str,
        opts: GetEventsOpts,
    ) -> EventStream<'_, ResourceRawEvent> {
        let schema = schema.to_string();
        self.stream_events(&opts, move |event| event.query.schema.as_str() == schema)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
