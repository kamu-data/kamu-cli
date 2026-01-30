// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::{EventID, EventStore, EventStoreStateImpl, GetEventsOpts, InMemoryEventStore};
use internal_error::InternalError;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type AccountQuotaEventStoreState = EventStoreStateImpl<AccountQuotaState>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryAccountQuotaEventStore {
    inner: InMemoryEventStore<AccountQuotaState, AccountQuotaEventStoreState>,
}

#[dill::component(pub)]
#[dill::interface(dyn AccountQuotaEventStore)]
#[dill::scope(dill::Singleton)]
impl InMemoryAccountQuotaEventStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<AccountQuotaState> for InMemoryAccountQuotaEventStore {
    fn get_all_events(
        &self,
        opts: GetEventsOpts,
    ) -> event_sourcing::EventStream<'_, AccountQuotaEvent> {
        self.inner.get_all_events(opts)
    }

    fn get_events(
        &self,
        query: &AccountQuotaQuery,
        opts: GetEventsOpts,
    ) -> event_sourcing::EventStream<'_, AccountQuotaEvent> {
        self.inner.get_events(query, opts)
    }

    async fn save_events(
        &self,
        query: &AccountQuotaQuery,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<AccountQuotaEvent>,
    ) -> Result<EventID, event_sourcing::SaveEventsError> {
        self.inner
            .save_events(query, maybe_prev_stored_event_id, events)
            .await
    }

    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccountQuotaEventStore for InMemoryAccountQuotaEventStore {
    async fn save_quota_events(
        &self,
        query: &AccountQuotaQuery,
        maybe_prev_event_id: Option<EventID>,
        events: Vec<AccountQuotaEvent>,
    ) -> Result<EventID, SaveAccountQuotaError> {
        self.save_events(query, maybe_prev_event_id, events)
            .await
            .map_err(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
