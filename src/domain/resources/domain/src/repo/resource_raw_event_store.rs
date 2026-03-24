// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{EventID, GetEventsOpts, SaveEventsError};
use internal_error::InternalError;

use crate::{ResourceRawEvent, ResourceRawEventQuery, ResourceRawEventStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceRawEventStore: Send + Sync {
    /// Returns the number of events stored
    async fn total_events_stored(&self) -> Result<usize, InternalError>;

    /// Returns the event history of all aggregates in chronological order
    fn get_all_events(&self, opts: GetEventsOpts) -> ResourceRawEventStream<'_>;

    /// Returns the event history of an aggregate in chronological order
    fn get_events(
        &self,
        query: &ResourceRawEventQuery,
        opts: GetEventsOpts,
    ) -> ResourceRawEventStream<'_>;

    /// Persists a series of events
    ///
    /// The `query` argument must be the same as query passed when retrieving
    /// the events. It will be used prior to saving events to ensure that there
    /// were no concurrent updates that could've influenced this transaction.
    async fn save_events(
        &self,
        query: &ResourceRawEventQuery,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<ResourceRawEvent>,
    ) -> Result<EventID, SaveEventsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
