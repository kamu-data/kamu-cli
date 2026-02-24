// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use internal_error::InternalError;
use messaging_outbox::*;

use crate::InMemoryMessageStoreWakeupDetector;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryOutboxMessageBridge {
    state: Mutex<State>,
    wakeup_detector: InMemoryMessageStoreWakeupDetector,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    messages: Vec<OutboxMessage>,
    // channel key -> applied message ids
    applied: HashMap<ChannelKey, BTreeSet<OutboxMessageID>>,
    // channel key -> next scan position in `merged`
    next_pos: HashMap<ChannelKey, usize>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
struct ChannelKey {
    producer_name: &'static str,
    consumer_name: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn OutboxMessageBridge)]
impl InMemoryOutboxMessageBridge {
    pub fn new() -> Self {
        let wakeup_detector = InMemoryMessageStoreWakeupDetector::new();

        Self {
            state: Mutex::new(State::default()),
            wakeup_detector,
        }
    }

    pub(crate) fn save_message(&self, new_message: &NewOutboxMessage) -> OutboxMessageID {
        let mut state = self.state.lock().unwrap();

        let message_id = state.messages.len() + 1;
        let message = new_message.as_outbox_message(
            OutboxMessageID::new(i64::try_from(message_id).unwrap()),
            0, /* no tx_id in-memory */
        );
        state.messages.push(message);

        let max_event_id = OutboxMessageID::new(i64::try_from(state.messages.len()).unwrap());

        // Wake up listeners
        self.wakeup_detector.notify_new_message_arrived();

        max_event_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl OutboxMessageBridge for InMemoryOutboxMessageBridge {
    /// Provides outbox message store wakeup detector instance
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector {
        &self.wakeup_detector
    }

    /// Fetch next batch for the given producer-consumer pair;
    ///  order by global id.
    async fn fetch_next_batch(
        &self,
        _: &dill::Catalog,
        producer_name: &'static str,
        consumer_name: &'static str,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError> {
        let mut state = self.state.lock().unwrap();

        let channel_key = ChannelKey {
            producer_name,
            consumer_name,
        };

        let pos = state.next_pos.get(&channel_key).copied().unwrap_or(0);
        let applied = state.applied.entry(channel_key).or_default().clone();

        let mut res = Vec::with_capacity(batch_size);
        let mut i = pos;

        while i < state.messages.len() && res.len() < batch_size {
            let e = &state.messages[i];

            if !applied.contains(&e.message_id) {
                res.push(e.clone());
            }
            i += 1;
        }

        // Advance the scan cursor to where we stopped scanning.
        // (Safe because we only ever consume in order.)
        state.next_pos.insert(channel_key, i);

        Ok(res)
    }

    /// Mark these messages as applied for this producer-consumer pair
    /// (should be idempotent!).
    async fn mark_applied(
        &self,
        _: &dill::Catalog,
        producer_name: &'static str,
        consumer_name: &'static str,
        message_ids_with_tx_ids: &[(OutboxMessageID, i64)],
    ) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();

        let channel_key = ChannelKey {
            producer_name,
            consumer_name,
        };

        // In-memory implementation does not care about
        let set = state.applied.entry(channel_key).or_default();
        for (id, _) in message_ids_with_tx_ids {
            set.insert(*id);
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
