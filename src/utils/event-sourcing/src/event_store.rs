// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use internal_error::{ErrorIntoInternal, InternalError};

use crate::{EventID, Projection, ProjectionEvent};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Common set of operations for an event store
#[async_trait::async_trait]
pub trait EventStore<Proj: Projection>: Send + Sync {
    /// Returns the number of events stored
    async fn total_events_stored(&self) -> Result<usize, InternalError>;

    /// Returns the event history of all aggregates in chronological order
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, Proj::Event>;

    /// Returns the event history of an aggregate in chronological order
    fn get_events(&self, query: &Proj::Query, opts: GetEventsOpts) -> EventStream<'_, Proj::Event>;

    /// Returns event history of multiple aggregates in chronological order
    /// Created to give a room for query optimisations when needed
    fn get_events_multi(
        &self,
        queries: &[Proj::Query],
    ) -> MultiEventStream<'_, Proj::Query, Proj::Event> {
        use tokio_stream::StreamExt;
        let queries = queries.to_vec();

        Box::pin(async_stream::try_stream! {
          for query in queries {
            let mut stream = self.get_events(&query, GetEventsOpts::default());
            while let Some(event) = stream.next().await {
              let (event_id, event) = event?;
              yield (query.clone(), event_id, event)
            }
          }
        })
    }

    /// Persists a series of events
    ///
    /// The `query` argument must be the same as query passed when retrieving
    /// the events. It will be used prior to saving events to ensure that there
    /// were no concurrent updates that could've influenced this transaction.
    async fn save_events(
        &self,
        query: &Proj::Query,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<Proj::Event>,
    ) -> Result<EventID, SaveEventsError>;

    /// Persists event batches for multiple aggregates.
    ///
    /// Returns last stored event ID for every item, preserving input order.
    async fn save_events_multi(
        &self,
        items: Vec<SaveEventsItem<Proj::Query, Proj::Event>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        // This is a fallback implementation that saves events for each aggregate
        // separately. It can be optimized by particular event stores.

        let mut event_ids = Vec::with_capacity(items.len());

        for item in items {
            if item.events.is_empty() {
                return Err(SaveEventsError::NothingToSave);
            }

            let event_id = self
                .save_events(&item.query, item.maybe_prev_stored_event_id, item.events)
                .await?;
            event_ids.push(event_id);
        }

        Ok(event_ids)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type EventStream<'a, Event> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<(EventID, Event), GetEventsError>> + Send + 'a>,
>;

pub type MultiEventStream<'a, Query, Event> = std::pin::Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<(Query, EventID, Event), GetEventsError>>
            + Send
            + 'a,
    >,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SaveEventsItem<Query, Event> {
    pub query: Query,
    pub maybe_prev_stored_event_id: Option<EventID>,
    pub events: Vec<Event>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct GetEventsOpts {
    /// Exclusive lower bound - to get events with IDs greater to this
    pub from: Option<EventID>,
    /// Inclusive upper bound - get events with IDs less or equal to this
    pub to: Option<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum GetEventsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum SaveEventsError {
    #[error("No events for saves")]
    NothingToSave,

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl SaveEventsError {
    pub fn concurrent_modification() -> Self {
        Self::ConcurrentModification(ConcurrentModificationError {})
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Concurrent modification")]
pub struct ConcurrentModificationError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Validates items before a multi-save operation.
///
/// Checks that:
/// - no item has an empty event list
/// - no two items share the same query
/// - every event's query matches its containing item's query
pub fn validate_multi_save_items<Query, Event>(
    items: &[SaveEventsItem<Query, Event>],
) -> Result<(), SaveEventsError>
where
    Query: Eq + std::hash::Hash + Clone + std::fmt::Debug,
    Event: ProjectionEvent<Query>,
{
    let mut seen_queries = HashSet::new();

    for item in items {
        if item.events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        if !seen_queries.insert(item.query.clone()) {
            return Err(SaveEventsError::Internal(
                format!("Duplicate query in multi-save: {:?}", item.query).int_err(),
            ));
        }

        for event in &item.events {
            if !event.matches_query(&item.query) {
                return Err(SaveEventsError::Internal(
                    format!("Event query does not match save query: {:?}", item.query).int_err(),
                ));
            }
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Maps event IDs assigned by one bulk insert of a multi-save back to its
/// items.
///
/// The events must have been inserted in item order, so that sorted IDs follow
/// that order too. Returns the last event ID of every item, preserving input
/// order.
pub fn last_event_ids_per_item(
    mut inserted_event_ids: Vec<i64>,
    item_event_counts: impl IntoIterator<Item = usize>,
) -> Result<Vec<EventID>, InternalError> {
    inserted_event_ids.sort_unstable();

    let mut last_event_ids = Vec::new();
    let mut num_events_seen = 0;

    for item_event_count in item_event_counts {
        num_events_seen += item_event_count;
        let Some(last_event_id) = num_events_seen
            .checked_sub(1)
            .and_then(|i| inserted_event_ids.get(i))
        else {
            return Err(format!(
                "Bulk insert returned {} event IDs, expected at least {num_events_seen}",
                inserted_event_ids.len()
            )
            .int_err());
        };
        last_event_ids.push(EventID::new(*last_event_id));
    }

    if num_events_seen != inserted_event_ids.len() {
        return Err(format!(
            "Bulk insert returned {} event IDs, expected {num_events_seen}",
            inserted_event_ids.len()
        )
        .int_err());
    }

    Ok(last_event_ids)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
