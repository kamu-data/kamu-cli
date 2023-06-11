// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use super::errors::*;
use crate::{Aggregate, AggregateExt, EventID};

/////////////////////////////////////////////////////////////////////////////////////////

/// Common set of operations for an event store
#[async_trait::async_trait]
pub trait EventStore: Send + Sync
where
    Self::Agg: Send,
    <Self::Agg as Aggregate>::Event: Send,
{
    type Agg: Aggregate;

    /// Returns the number of events stored
    async fn len(&self) -> Result<usize, InternalError>;

    /// Returns the event history of an aggregate in chronological order
    fn get_events<'a>(
        &'a self,
        id: &<Self::Agg as Aggregate>::Id,
        opts: GetEventsOpts,
    ) -> EventStream<'a, <Self::Agg as Aggregate>::Event>;

    // TODO: Concurrency control
    /// Persists a series of events
    async fn save_events(
        &self,
        events: Vec<<Self::Agg as Aggregate>::Event>,
    ) -> Result<EventID, SaveError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Convenience methods for [EventStore] implementations
#[async_trait::async_trait]
pub trait EventStoreExt: EventStore {
    /// Initializes an aggregate from event history
    async fn load(
        &self,
        id: &<Self::Agg as Aggregate>::Id,
    ) -> Result<Self::Agg, LoadError<Self::Agg>> {
        self.load_ext(id, LoadOpts::default()).await
    }

    /// Same as [EventStore::load()] but with extra control knobs
    async fn load_ext(
        &self,
        id: &<Self::Agg as Aggregate>::Id,
        opts: LoadOpts,
    ) -> Result<Self::Agg, LoadError<Self::Agg>> {
        let event_stream = self.get_events(
            id,
            GetEventsOpts {
                from: None,
                to: opts.as_of_event,
            },
        );
        match AggregateExt::from_event_stream(event_stream).await? {
            Some(agg) => Ok(agg),
            None => Err(AggrateNotFoundError::new(id.clone()).into()),
        }
    }

    /// Updates the state of an aggregate with events that happened since the
    /// last load.
    ///
    /// Will panic if the aggregate has pending updates
    async fn update(&self, agg: &mut Self::Agg) -> Result<(), UpdateError<Self::Agg>> {
        self.update_ext(agg, LoadOpts::default()).await
    }

    /// Same as [EventStore::update()] but with extra control knobs
    async fn update_ext(
        &self,
        agg: &mut Self::Agg,
        opts: LoadOpts,
    ) -> Result<(), UpdateError<Self::Agg>> {
        assert!(!agg.has_updates());
        let event_stream = self.get_events(
            agg.id(),
            GetEventsOpts {
                from: agg.last_synced_event().cloned(),
                to: opts.as_of_event,
            },
        );
        agg.mutate_stream(event_stream).await
    }

    /// Persists pending aggregate events
    async fn save(&self, agg: &mut Self::Agg) -> Result<(), SaveError> {
        let events = agg.updates();
        let event_id = self.save_events(events).await?;
        agg.update_last_synced_event(event_id);
        Ok(())
    }
}

// Blanket impl
impl<T: ?Sized> EventStoreExt for T where T: EventStore {}

/////////////////////////////////////////////////////////////////////////////////////////

pub type EventStream<'a, Event> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<(EventID, Event), InternalError>> + Send + 'a>,
>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct GetEventsOpts {
    /// Exclusive lower bound - to get events with IDs greater to this
    pub from: Option<EventID>,
    /// Inclusive upper bound - get events with IDs less or equal to this
    pub to: Option<EventID>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct LoadOpts {
    /// Only considers a subset of events (inclusive upper bound)
    pub as_of_event: Option<EventID>,
}
