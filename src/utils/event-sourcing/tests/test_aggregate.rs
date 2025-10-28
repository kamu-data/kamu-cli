// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use event_sourcing::*;

#[derive(Debug, Clone)]
enum CalcEvents {
    Add(i32),
    Sub(i32),
}

impl ProjectionEvent<()> for CalcEvents {
    fn matches_query(&self, _: &()) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct CalcState(i32);

impl Projection for CalcState {
    type Query = ();
    type Event = CalcEvents;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use CalcEvents::*;
        match (state, event) {
            (None, Add(v)) => Ok(Self(v)),
            (None, Sub(v)) => Ok(Self(-v)),
            (Some(s), Add(v)) => Ok(Self(s.0 + v)),
            (Some(s), Sub(v)) => Ok(Self(s.0 - v)),
        }
    }
}

struct CalcEventStore(Mutex<Vec<CalcEvents>>);

impl CalcEventStore {
    fn new(events: Vec<CalcEvents>) -> Self {
        Self(Mutex::new(events))
    }
}

#[async_trait::async_trait]
impl EventStore<CalcState> for CalcEventStore {
    fn get_all_events(&self, _opts: GetEventsOpts) -> EventStream<'_, CalcEvents> {
        use futures::StreamExt;
        Box::pin(
            tokio_stream::iter(self.0.lock().unwrap().clone())
                .enumerate()
                .map(|(i, e)| Ok((EventID::new(i64::try_from(i).unwrap()), e))),
        )
    }

    fn get_events(&self, _query: &(), _opts: GetEventsOpts) -> EventStream<'_, CalcEvents> {
        use futures::StreamExt;
        Box::pin(
            tokio_stream::iter(self.0.lock().unwrap().clone())
                .enumerate()
                .map(|(i, e)| Ok((EventID::new(i64::try_from(i).unwrap()), e))),
        )
    }

    async fn save_events(
        &self,
        _query: &(),
        _prev_stored_event_id: Option<EventID>,
        mut events: Vec<CalcEvents>,
    ) -> Result<EventID, SaveEventsError> {
        let mut s = self.0.lock().unwrap();
        s.append(&mut events);
        Ok(EventID::new(i64::try_from(s.len() - 1).unwrap()))
    }

    async fn len(&self) -> Result<usize, InternalError> {
        Ok(self.0.lock().unwrap().len())
    }
}

#[derive(Aggregate, Debug)]
struct Calc(Aggregate<CalcState, CalcEventStore>);

#[tokio::test]
async fn test_aggregate_load() {
    let store = CalcEventStore::new(vec![CalcEvents::Add(10), CalcEvents::Sub(6)]);
    let c = Calc::load((), &store).await.unwrap();
    assert_eq!(c.as_ref().0, 4);
    assert_eq!(c.last_stored_event_id(), Some(EventID::new(1)));
}

#[tokio::test]
async fn test_aggregate_debug() {
    let store = CalcEventStore::new(vec![CalcEvents::Add(1), CalcEvents::Add(1)]);
    let actual = format!("{:?}", Calc::load((), &store).await.unwrap());
    assert_eq!(
        "Calc(Aggregate { query: (), state: CalcState(2), pending_events: [], \
         last_stored_event_id: Some(1) })",
        actual
    );
}
