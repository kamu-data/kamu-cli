// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{EventID, Projection};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Common set of operations for an event store
#[allow(clippy::len_without_is_empty)]
#[async_trait::async_trait]
pub trait EventStore<Proj: Projection>: Send + Sync {
    /// Returns the event history of an aggregate in chronological order
    fn get_events(&self, query: &Proj::Query, opts: GetEventsOpts) -> EventStream<Proj::Event>;

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

    /// Returns the number of events stored
    async fn len(&self) -> Result<usize, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type EventStream<'a, Event> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<(EventID, Event), GetEventsError>> + Send + 'a>,
>;

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
