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

/////////////////////////////////////////////////////////////////////////////////////////

/// Common set of operations for an event-sourced aggregate
#[async_trait::async_trait]
pub trait Aggregate
where
    Self: Sized,
{
    type Id;
    type EventType;
    type ProjectionType;
    type EventStore: ?Sized;

    /// Initializes an aggregate from projected state
    fn from_state(state: Self::ProjectionType) -> Self;

    /// Initializes an aggregate from projected state
    fn from_genesis_event(
        event: Self::EventType,
    ) -> Result<Self, ProjectionError<Self::ProjectionType, Self::EventType>>;

    /// Initializes an aggregate from event stream
    async fn from_event_stream<Stream, Event>(
        mut event_stream: Stream,
    ) -> Result<Option<Self>, LoadError<Self::ProjectionType, Self::EventType>>
    where
        Stream: tokio_stream::Stream<Item = Result<Event, InternalError>> + Send + Unpin,
        Event: Into<Self::EventType> + Send,
    {
        use tokio_stream::StreamExt;

        let genesis = match event_stream.next().await {
            None => return Ok(None),
            Some(Ok(event)) => event,
            Some(Err(err)) => return Err(err.into()),
        };

        let mut agg = Self::from_genesis_event(genesis.into())?;

        while let Some(res) = event_stream.next().await {
            let event = res?;
            agg.apply_no_save(event.into())?;
        }

        Ok(Some(agg))
    }

    /// Initializes an aggregate from the event store
    async fn load(
        id: &Self::Id,
        event_store: &Self::EventStore,
    ) -> Result<Option<Self>, LoadError<Self::ProjectionType, Self::EventType>>;

    /// Saves pending events into event store
    async fn save(&mut self, event_store: &Self::EventStore) -> Result<(), SaveError>;

    /// Update current state projection with an event and save it in the pending
    /// event list
    fn apply(
        &mut self,
        event: Self::EventType,
    ) -> Result<(), IllegalSequenceError<Self::ProjectionType, Self::EventType>>;

    /// Update current state projection with an event without saving it in the
    /// pending event list (used when initializing an aggregate)
    fn apply_no_save(
        &mut self,
        event: Self::EventType,
    ) -> Result<(), IllegalSequenceError<Self::ProjectionType, Self::EventType>>;

    /// Unique identity of an aggregate
    fn id(&self) -> &Self::Id;

    /// Casts an aggregate into projected state
    fn as_state(&self) -> &Self::ProjectionType;

    /// Converts an aggregate into projected state.
    ///
    /// All pending events have to be saved or the call will panic.
    fn into_state(self) -> Self::ProjectionType;
}

/////////////////////////////////////////////////////////////////////////////////////////
