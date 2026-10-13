// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{
    EventStore,
    EventStream,
    GetEventsOpts,
    Projection,
    ProjectionError,
    ProjectionEvent,
};
use internal_error::InternalError;

use crate::{ResourceRawEvent, ResourceRawEventQuery};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceRawEventStore: EventStore<ResourceRawEventProjection> {
    async fn total_events_stored_by_schema(&self, schema: &str) -> Result<usize, InternalError>;

    fn get_all_events_by_schema(
        &self,
        schema: &str,
        opts: GetEventsOpts,
    ) -> EventStream<'_, ResourceRawEvent>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceRawEventProjection {
    raw_events: Vec<ResourceRawEvent>,
}

impl Projection for ResourceRawEventProjection {
    type Event = ResourceRawEvent;
    type Query = ResourceRawEventQuery;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        let mut raw_events = state.map(|s| s.raw_events).unwrap_or_default();
        raw_events.push(event);
        Ok(Self { raw_events })
    }
}

impl ProjectionEvent<ResourceRawEventQuery> for ResourceRawEvent {
    fn matches_query(&self, query: &ResourceRawEventQuery) -> bool {
        self.query == *query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
