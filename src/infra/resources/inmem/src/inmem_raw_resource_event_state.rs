// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicI64, Ordering};

use database_common::{
    InMemoryTransactionParticipant,
    InMemoryTransactionalMap,
    TransactionId,
    in_memory_transaction_participant,
};
use dill::*;
use event_sourcing::{EventID, GetEventsOpts, SaveEventsError, SaveEventsItem};
use kamu_resources::{ResourceRawEvent, ResourceRawEventQuery};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resource events as a database keeps them: an ID is taken from a sequence
/// when an event is saved, and a rolled-back transaction leaves a gap. The
/// last event of each resource is a key of its own, so two transactions
/// appending to one resource conflict at commit, as the expected-last-event
/// check of the SQL backends makes the later one fail
pub struct InMemoryRawResourceEventState {
    events_by_id: InMemoryTransactionalMap<EventID, ResourceRawEvent>,
    last_event_id_by_query: InMemoryTransactionalMap<ResourceRawEventQuery, EventID>,
    last_event_id: AtomicI64,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryRawResourceEventState {
    pub fn new() -> Self {
        Self {
            events_by_id: InMemoryTransactionalMap::new(),
            last_event_id_by_query: InMemoryTransactionalMap::new(),
            last_event_id: AtomicI64::new(0),
        }
    }

    /// The matching events the transaction sees within `opts`, ordered by ID
    pub(crate) fn events(
        &self,
        transaction_id: Option<TransactionId>,
        opts: &GetEventsOpts,
        matches: impl Fn(&ResourceRawEvent) -> bool,
    ) -> Vec<ResourceRawEvent> {
        let mut events = self.events_by_id.read(transaction_id, |events_by_id| {
            events_by_id
                .iter()
                .filter(|(event_id, _)| opts.from.is_none_or(|from| **event_id > from))
                .filter(|(event_id, _)| opts.to.is_none_or(|to| **event_id <= to))
                .map(|(_, event)| event)
                .filter(|event| matches(event))
                .cloned()
                .collect::<Vec<_>>()
        });
        events.sort_by_key(|event| event.event_id);
        events
    }

    pub(crate) fn events_count(
        &self,
        transaction_id: Option<TransactionId>,
        matches: impl Fn(&ResourceRawEvent) -> bool,
    ) -> usize {
        self.events_by_id.read(transaction_id, |events_by_id| {
            events_by_id
                .iter()
                .filter(|(_, event)| matches(event))
                .count()
        })
    }

    /// Appends the events of every item, provided each resource's last event
    /// is still the expected one. Returns the last event ID of each item
    pub(crate) fn save(
        &self,
        transaction_id: Option<TransactionId>,
        items: Vec<SaveEventsItem<ResourceRawEventQuery, ResourceRawEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        self.events_by_id.write(transaction_id, |events_by_id| {
            self.last_event_id_by_query
                .write(transaction_id, |last_event_id_by_query| {
                    let last_event_ids = last_event_id_by_query.view();
                    if items.iter().any(|item| {
                        last_event_ids.get(&item.query).copied() != item.maybe_prev_stored_event_id
                    }) {
                        return Err(SaveEventsError::concurrent_modification());
                    }

                    let mut result = Vec::with_capacity(items.len());
                    for item in items {
                        let mut last_event_id = None;
                        for mut event in item.events {
                            let event_id = self.next_event_id();
                            event.event_id = event_id;
                            events_by_id.put(event_id, event);
                            last_event_id = Some(event_id);
                        }

                        let last_event_id = last_event_id.ok_or(SaveEventsError::NothingToSave)?;
                        last_event_id_by_query.put(item.query, last_event_id);
                        result.push(last_event_id);
                    }

                    Ok(result)
                })
        })
    }

    fn next_event_id(&self) -> EventID {
        EventID::new(self.last_event_id.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

in_memory_transaction_participant!(
    InMemoryRawResourceEventState,
    events_by_id,
    last_event_id_by_query
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
