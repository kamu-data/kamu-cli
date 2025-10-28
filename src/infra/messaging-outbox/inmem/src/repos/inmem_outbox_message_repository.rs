// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use dill::{Singleton, component, interface, scope};
use internal_error::InternalError;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryOutboxMessageRepository {
    state: Arc<Mutex<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    last_message_id: Option<OutboxMessageID>,
    messages: BTreeMap<OutboxMessageID, OutboxMessage>,
    latest_message_id_by_producer: HashMap<String, OutboxMessageID>,
}

impl State {
    fn next_message_id(&mut self) -> OutboxMessageID {
        let next_message_id = if let Some(last_message_id) = self.last_message_id {
            let id = last_message_id.into_inner();
            OutboxMessageID::new(id + 1)
        } else {
            OutboxMessageID::new(1)
        };
        self.last_message_id = Some(next_message_id);
        next_message_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn OutboxMessageRepository)]
impl InMemoryOutboxMessageRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl OutboxMessageRepository for InMemoryOutboxMessageRepository {
    async fn push_message(&self, new_message: NewOutboxMessage) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        let message_id: OutboxMessageID = guard.next_message_id();

        guard
            .latest_message_id_by_producer
            .insert(new_message.producer_name.clone(), message_id);

        guard
            .messages
            .insert(message_id, new_message.as_outbox_message(message_id));
        Ok(())
    }

    fn get_messages(
        &self,
        above_boundaries_by_producer: Vec<(String, OutboxMessageID)>,
        batch_size: usize,
    ) -> OutboxMessageStream<'_> {
        let minimal_above_id = above_boundaries_by_producer
            .iter()
            .map(|(_, boundary_id)| boundary_id)
            .min()
            .copied()
            .unwrap_or_else(|| OutboxMessageID::new(0));

        let messages = {
            let mut messages = Vec::new();

            let guard = self.state.lock().unwrap();

            let mut cursor = guard
                .messages
                .lower_bound(Bound::Excluded(&minimal_above_id));

            while let Some((_, message)) = cursor.next() {
                let matches_filter =
                    above_boundaries_by_producer
                        .iter()
                        .any(|(producer_name, above_id)| {
                            message.producer_name == *producer_name
                                && message.message_id > *above_id
                        });

                if matches_filter || above_boundaries_by_producer.is_empty() {
                    messages.push(Ok(message.clone()));
                    if messages.len() >= batch_size {
                        break;
                    }
                }
            }

            messages
        };

        Box::pin(tokio_stream::iter(messages))
    }

    async fn get_latest_message_ids_by_producer(
        &self,
    ) -> Result<Vec<(String, OutboxMessageID)>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .latest_message_id_by_producer
            .iter()
            .map(|e| (e.0.clone(), *(e.1)))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
