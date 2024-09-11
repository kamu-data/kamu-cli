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
}

#[async_trait::async_trait]
impl<Proj: Projection, State: EventStoreState<Proj>> EventStore<Proj>
    for InMemoryEventStore<Proj, State>
{
    async fn len(&self) -> Result<usize, InternalError> {
        Ok(self.state.lock().unwrap().events_count())
    }

    fn get_events(&self, query: &Proj::Query, opts: GetEventsOpts) -> EventStream<Proj::Event> {
        let query = query.clone();

        // TODO: This should be a buffered stream so we don't lock per event
        Box::pin(async_stream::try_stream! {
            let mut seen = opts.from.map_or(0, |id| usize::try_from(id.into_inner() + 1).unwrap());

            loop {
                let next = {
                    let g = self.state.lock().unwrap();
                    let to = opts.to.map_or(g.events_count(), |id| usize::try_from(id.into_inner() + 1).unwrap());

                    g.get_events()[..to]
                        .iter()
                        .enumerate()
                        .skip(seen)
                        .filter(|(_, e)| e.matches_query(&query))
                        .map(|(i, e)| (i, e.clone()))
                        .next()
                };

                match next {
                    None => break,
                    Some((i, event)) => {
                        seen = i + 1;
                        yield (EventID::new(i64::try_from(i).unwrap()), event)
                    }
                }
            }
        })
    }

    // TODO: concurrency
    async fn save_events(
        &self,
        _: &Proj::Query,
        _: Option<EventID>,
        events: Vec<Proj::Event>,
    ) -> Result<EventID, SaveEventsError> {
        let mut g = self.state.lock().unwrap();

        for event in events {
            g.add_event(event);
        }

        Ok(EventID::new(i64::try_from(g.events_count() - 1).unwrap()))
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
