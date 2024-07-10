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

use dill::{component, interface, scope, Singleton};
use internal_error::InternalError;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxMessageRepositoryInMemory {
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
            OutboxMessageID::new(0)
        };
        self.last_message_id = Some(next_message_id);
        next_message_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn OutboxMessageRepository)]
impl OutboxMessageRepositoryInMemory {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl OutboxMessageRepository for OutboxMessageRepositoryInMemory {
    async fn push_message(&self, new_message: NewOutboxMessage) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        let message_id: OutboxMessageID = guard.next_message_id();

        guard
            .latest_message_id_by_producer
            .insert(new_message.producer_name.clone(), message_id);

        guard.messages.insert(
            message_id,
            OutboxMessage {
                message_id,
                message_type: new_message.message_type,
                producer_name: new_message.producer_name,
                content_json: new_message.content_json,
                occurred_on: new_message.occurred_on,
            },
        );
        Ok(())
    }

    async fn get_producer_messages(
        &self,
        producer_name: &str,
        above_id: OutboxMessageID,
        batch_size: usize,
    ) -> Result<OutboxMessageStream, InternalError> {
        let messages = {
            let mut messages = Vec::new();

            let guard = self.state.lock().unwrap();

            let mut cursor = guard.messages.lower_bound(Bound::Excluded(&above_id));
            while let Some((_, message)) = cursor.next() {
                if message.producer_name == producer_name {
                    messages.push(Ok(message.clone()));
                    if messages.len() >= batch_size {
                        break;
                    }
                }
            }

            messages
        };

        Ok(Box::pin(tokio_stream::iter(messages)))
    }

    async fn get_largest_message_id_recorded(
        &self,
    ) -> Result<Option<OutboxMessageID>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard.last_message_id)
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
