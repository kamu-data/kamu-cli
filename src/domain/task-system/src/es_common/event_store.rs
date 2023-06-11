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
use crate::{Aggregate, EventID};

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
    #[tracing::instrument(
        level = "debug",
        name = "load",
        skip_all,
        fields(
            agg_type = %std::any::type_name::<Self::Agg>(),
            agg_id = %id,
        )
    )]
    async fn load_ext(
        &self,
        id: &<Self::Agg as Aggregate>::Id,
        opts: LoadOpts,
    ) -> Result<Self::Agg, LoadError<Self::Agg>> {
        use tokio_stream::StreamExt;

        let mut event_stream = self.get_events(
            id,
            GetEventsOpts {
                from: None,
                to: opts.as_of_event,
            },
        );

        let (event_id, event) = match event_stream.next().await {
            Some(Ok(v)) => v,
            Some(Err(err)) => return Err(err.into()),
            None => return Err(AggrateNotFoundError::new(id.clone()).into()),
        };

        let mut agg = Self::Agg::from_genesis_event(event_id, event)?;
        let mut num_events = 1;

        while let Some(res) = event_stream.next().await {
            let (event_id, event) = res?;
            agg.mutate(event_id, event)?;
            num_events += 1;
        }

        tracing::debug!(
            num_events,
            last_synced_event = %agg.last_synced_event().unwrap(),
            "Loaded aggregate",
        );

        Ok(agg)
    }

    /// Updates the state of an aggregate with events that happened since the
    /// last load.
    ///
    /// Will panic if the aggregate has pending updates
    async fn update(&self, agg: &mut Self::Agg) -> Result<(), UpdateError<Self::Agg>> {
        self.update_ext(agg, LoadOpts::default()).await
    }

    /// Same as [EventStore::update()] but with extra control knobs
    #[tracing::instrument(
        level = "debug",
        name = "update",
        skip_all,
        fields(
            agg_type = %std::any::type_name::<Self::Agg>(),
            agg_id = %agg.id(),
        )
    )]
    async fn update_ext(
        &self,
        agg: &mut Self::Agg,
        opts: LoadOpts,
    ) -> Result<(), UpdateError<Self::Agg>> {
        use tokio_stream::StreamExt;

        assert!(!agg.has_updates());

        let prev_synced_event = agg.last_synced_event().cloned();

        let mut event_stream = self.get_events(
            agg.id(),
            GetEventsOpts {
                from: prev_synced_event,
                to: opts.as_of_event,
            },
        );

        let mut num_events = 1;

        while let Some(res) = event_stream.next().await {
            let (event_id, event) = res?;
            agg.mutate(event_id, event)?;
            num_events += 1;
        }

        tracing::debug!(
            num_events,
            prev_synced_event = ?prev_synced_event,
            last_synced_event = %agg.last_synced_event().unwrap(),
            "Updated aggregate",
        );

        Ok(())
    }

    /// Persists pending aggregate events
    #[tracing::instrument(
        level = "debug",
        name = "save",
        skip_all, fields(
            agg_type = %std::any::type_name::<Self::Agg>(),
            agg_id = %agg.id(),
        )
    )]
    async fn save(&self, agg: &mut Self::Agg) -> Result<(), SaveError> {
        let events = agg.updates();

        if events.len() != 0 {
            let num_events = events.len();
            let prev_synced_event = agg.last_synced_event().cloned();

            let last_event_id = self.save_events(events).await?;
            agg.update_last_synced_event(last_event_id);

            tracing::debug!(
                num_events,
                prev_synced_event = ?prev_synced_event,
                last_synced_event = %last_event_id,
                "Saved aggregate",
            );
        }

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
