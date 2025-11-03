// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use internal_error::InternalError;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryEventStore<Proj: Projection, State: EventStoreState<Proj>> {
    state: Arc<Mutex<State>>,
    _proj: PhantomData<Proj>,
}

impl<Proj: Projection, State: EventStoreState<Proj>> InMemoryEventStore<Proj, State> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            _proj: PhantomData,
        }
    }

    pub fn as_state(&self) -> Arc<Mutex<State>> {
        self.state.clone()
    }

    fn detect_concurrent_modification(
        &self,
        state: &State,
        query: &Proj::Query,
        maybe_prev_stored_event_id: Option<EventID>,
    ) -> Result<(), SaveEventsError> {
        // Find last actually stored event that matches the given query.
        // We can compute it's reverse index (index from the tail)
        let maybe_last_stored_event_rindex = state
            .get_events()
            .iter()
            .rev()
            .enumerate()
            .find(|(_, e)| e.matches_query(query))
            .map(|(i, _)| i);

        // 2 valid cases:
        //  - we have an event, and we expect an event with the same id
        //  - we don't have an event, and we don't expect an event too
        // Other cases are indicators of concurrent modifications
        match (maybe_prev_stored_event_id, maybe_last_stored_event_rindex) {
            // Both event and expectation
            (Some(prev_stored_event_id), Some(last_stored_event_rindex)) => {
                // Convert reverse index into normal index
                let total_events = state.events_count();
                let last_stored_event_id =
                    i64::try_from(total_events - last_stored_event_rindex).unwrap();

                // Compare index of the event with the expectation
                if last_stored_event_id != prev_stored_event_id.into_inner() {
                    return Err(SaveEventsError::concurrent_modification());
                }

                Ok(())
            }

            // Neither event, nor expectation
            (None, None) => Ok(()),

            // Other cases are invalid
            _ => Err(SaveEventsError::concurrent_modification()),
        }
    }
}

#[async_trait::async_trait]
impl<Proj: Projection, State: EventStoreState<Proj>> EventStore<Proj>
    for InMemoryEventStore<Proj, State>
{
    async fn len(&self) -> Result<usize, InternalError> {
        Ok(self.state.lock().unwrap().events_count())
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, Proj::Event> {
        // TODO: This should be a buffered stream so we don't lock per event
        Box::pin(async_stream::try_stream! {
            let mut seen = opts.from.map_or(0, |id| usize::try_from(id.into_inner()).unwrap());

            loop {
                let next = {
                    let g = self.state.lock().unwrap();

                    let total_events = g.events_count();
                    let to = opts.to
                        .map(|id| usize::try_from(id.into_inner()).unwrap())
                        .map(|to| to.min(total_events))
                        .unwrap_or(total_events);

                    g.get_events()[..to]
                        .iter()
                        .enumerate()
                        .skip(seen)
                        .map(|(i, e)| (i + 1, e.clone()))
                        .next()

                };

                match next {
                    None => break,
                    Some((i, event)) => {
                        seen = i;
                        yield (EventID::new(i64::try_from(i).unwrap()), event)
                    }
                }
            }
        })
    }

    fn get_events(&self, query: &Proj::Query, opts: GetEventsOpts) -> EventStream<'_, Proj::Event> {
        let query = query.clone();

        // TODO: This should be a buffered stream so we don't lock per event
        Box::pin(async_stream::try_stream! {
            let mut seen = opts.from.map_or(0, |id| usize::try_from(id.into_inner()).unwrap());

            loop {
                let next = {
                    let g = self.state.lock().unwrap();
                    let to = opts.to.map_or(g.events_count(), |id| usize::try_from(id.into_inner()).unwrap());

                    g.get_events()[..to]
                        .iter()
                        .enumerate()
                        .skip(seen)
                        .filter(|(_, e)| e.matches_query(&query))
                        .map(|(i, e)| (i + 1, e.clone()))
                        .next()
                };

                match next {
                    None => break,
                    Some((i, event)) => {
                        seen = i;
                        yield (EventID::new(i64::try_from(i).unwrap()), event)
                    }
                }
            }
        })
    }

    async fn save_events(
        &self,
        query: &Proj::Query,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<Proj::Event>,
    ) -> Result<EventID, SaveEventsError> {
        // Add events only if there was no concurrent modification
        let mut g = self.state.lock().unwrap();
        self.detect_concurrent_modification(&g, query, maybe_prev_stored_event_id)?;

        // Everything is fine, so commit the events
        for event in events {
            g.add_event(event);
        }

        // The id is computed from the index
        Ok(EventID::new(i64::try_from(g.events_count()).unwrap()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait EventStoreState<Proj: Projection>: Default + Sync + Send {
    fn events_count(&self) -> usize;

    fn get_events(&self) -> &[Proj::Event];

    fn add_event(&mut self, event: Proj::Event);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EventStoreStateImpl<Proj: Projection> {
    events: Vec<Proj::Event>,
}

impl<Proj: Projection> Default for EventStoreStateImpl<Proj> {
    fn default() -> Self {
        Self { events: Vec::new() }
    }
}

impl<Proj: Projection> EventStoreState<Proj> for EventStoreStateImpl<Proj> {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[Proj::Event] {
        &self.events
    }

    fn add_event(&mut self, event: Proj::Event) {
        self.events.push(event);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
