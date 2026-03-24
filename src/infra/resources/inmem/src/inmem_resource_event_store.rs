// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use database_common::PaginationOpts;
use dill::*;
use event_sourcing::{EventID, GetEventsOpts, SaveEventsError};
use internal_error::InternalError;
use kamu_resources::{
    ResourceID,
    ResourceIDStream,
    ResourceName,
    ResourceRawEvent,
    ResourceRawEventQuery,
    ResourceRawEventStore,
    ResourceRepository,
    ResourceRow,
    ResourceRawEventStream,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    last_event_id: Option<EventID>,
    events_by_query: HashMap<ResourceRawEventQuery, Vec<ResourceRawEvent>>,
}

impl State {
    fn next_event_id(&mut self) -> EventID {
        let next_id = if let Some(last_event_id) = self.last_event_id {
            EventID::new(last_event_id.into_inner() + 1)
        } else {
            EventID::new(0)
        };

        self.last_event_id = Some(next_id);
        next_id
    }

    fn get_last_event_id(&self, query: &ResourceRawEventQuery) -> Option<EventID> {
        self.events_by_query
            .get(query)
            .and_then(|events| events.last())
            .map(|event| event.event_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryResourceEventStore {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn ResourceRawEventStore)]
#[interface(dyn ResourceRepository)]
#[scope(Singleton)]
impl InMemoryResourceEventStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRepository for InMemoryResourceEventStore {
    async fn new_resource_id(&self) -> Result<ResourceID, InternalError> {
        Ok(ResourceID::new_v4())
    }

    async fn get_resource_id_by_name(
        &self,
        _account_id: odf::AccountID,
        _kind: &str,
        _name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError> {
        Err(InternalError::new(
            "Resource lookup by name is not implemented for the event-only in-memory store",
        ))
    }

    async fn get_resource_row(
        &self,
        _query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceRow>, InternalError> {
        Err(InternalError::new(
            "Resource snapshot rows are not implemented for the event-only in-memory store",
        ))
    }

    fn list_resource_ids(
        &self,
        _account_id: odf::AccountID,
        _kind: &str,
        _pagination: PaginationOpts,
    ) -> ResourceIDStream<'_> {
        Box::pin(futures::stream::iter([Err(InternalError::new(
            "Resource listing is not implemented for the event-only in-memory store",
        ))]))
    }

    async fn get_count_resources(
        &self,
        _account_id: odf::AccountID,
        _kind: &str,
    ) -> Result<usize, InternalError> {
        Err(InternalError::new(
            "Resource counting is not implemented for the event-only in-memory store",
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRawEventStore for InMemoryResourceEventStore {
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard.events_by_query.values().map(Vec::len).sum())
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> ResourceRawEventStream<'_> {
        let events_page = {
            let guard = self.state.lock().unwrap();
            let mut all_events: Vec<_> = guard
                .events_by_query
                .values()
                .flatten()
                .filter(|event| opts.from.is_none_or(|from| event.event_id > from))
                .filter(|event| opts.to.is_none_or(|to| event.event_id <= to))
                .cloned()
                .map(Ok)
                .collect();

            all_events.sort_by_key(|event| event.as_ref().unwrap().event_id);
            all_events
        };

        Box::pin(futures::stream::iter(events_page))
    }

    fn get_events(
        &self,
        query: &ResourceRawEventQuery,
        opts: GetEventsOpts,
    ) -> ResourceRawEventStream<'_> {
        let events_page = {
            let guard = self.state.lock().unwrap();

            guard
                .events_by_query
                .get(query)
                .map(|events| {
                    events
                        .iter()
                        .filter(|event| opts.from.is_none_or(|from| event.event_id > from))
                        .filter(|event| opts.to.is_none_or(|to| event.event_id <= to))
                        .cloned()
                        .map(Ok)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        Box::pin(futures::stream::iter(events_page))
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

        let mut guard = self.state.lock().unwrap();
        let actual_last_event_id = guard.get_last_event_id(query);

        if actual_last_event_id != maybe_prev_stored_event_id {
            return Err(SaveEventsError::concurrent_modification());
        }

        let mut last_event_id = None;

        for event in events {
            let event_id = guard.next_event_id();

            guard
                .events_by_query
                .entry(query.clone())
                .or_default()
                .push(ResourceRawEvent {
                    event_id,
                    query: query.clone(),
                    event_time: event.event_time,
                    event_type: event.event_type,
                    payload: event.payload,
                });

            last_event_id = Some(event_id);
        }

        Ok(last_event_id.expect("events is not empty"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
