// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
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
    // producer -> latest message id
    latest_message_id_by_producer: HashMap<String, OutboxMessageID>,
    // channel key -> consumed boundary
    consumed_boundaries: HashMap<ChannelKey, OutboxMessageBoundary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ChannelKey {
    producer_name: String,
    consumer_name: String,
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

    fn push_message_internal(&self, new_message: NewOutboxMessage) -> OutboxMessage {
        let mut state = self.state.lock().unwrap();

        let next_message_id = i64::try_from(state.messages.len() + 1).unwrap();
        let message_id = OutboxMessageID::new(next_message_id);

        let message = new_message.as_outbox_message(message_id, 0 /* no tx_id in-memory */);
        state
            .latest_message_id_by_producer
            .insert(new_message.producer_name, message_id);
        state.messages.push(message.clone());

        // Wake up listeners
        self.wakeup_detector.notify_new_message_arrived();

        message
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl OutboxMessageBridge for InMemoryOutboxMessageBridge {
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector {
        &self.wakeup_detector
    }

    async fn push_message(
        &self,
        _transaction_catalog: &dill::Catalog,
        message: NewOutboxMessage,
    ) -> Result<(), InternalError> {
        self.push_message_internal(message);
        Ok(())
    }

    fn get_unprocessed_messages(
        &self,
        _transaction_catalog: &dill::Catalog,
        above_boundaries_by_producer: Vec<(String, OutboxMessageBoundary)>,
        batch_size: usize,
    ) -> OutboxMessageStream<'_> {
        assert!(
            !above_boundaries_by_producer.is_empty(),
            "get_unprocessed_messages requires non-empty boundaries"
        );

        let minimal_above_id = above_boundaries_by_producer
            .iter()
            .map(|(_, boundary)| boundary.message_id) // ignore tx_id for in-memory implementation
            .min()
            .unwrap_or_else(|| OutboxMessageID::new(0));

        let messages = {
            let state = self.state.lock().unwrap();

            let mut collected = Vec::new();
            for message in state
                .messages
                .iter()
                .skip_while(|m| m.message_id <= minimal_above_id)
            {
                let matches_filter =
                    above_boundaries_by_producer
                        .iter()
                        .any(|(producer_name, above_boundary)| {
                            message.producer_name == *producer_name
                                && message.message_id > above_boundary.message_id // ignore tx_id for in-memory implementation
                        });

                if matches_filter {
                    collected.push(Ok(message.clone()));
                    if collected.len() >= batch_size {
                        break;
                    }
                }
            }

            collected
        };

        Box::pin(tokio_stream::iter(messages))
    }

    fn get_messages_by_producer(
        &self,
        _transaction_catalog: &dill::Catalog,
        producer_name: &str,
        above_boundary: OutboxMessageBoundary,
        batch_size: usize,
    ) -> OutboxMessageStream<'_> {
        let messages = {
            let state = self.state.lock().unwrap();
            state
                .messages
                .iter()
                .filter(|message| {
                    message.producer_name == producer_name
                        && message.message_id > above_boundary.message_id
                })
                .take(batch_size)
                .cloned()
                .map(Ok)
                .collect::<Vec<_>>()
        };

        Box::pin(tokio_stream::iter(messages))
    }

    async fn get_latest_message_boundaries_by_producer(
        &self,
        _transaction_catalog: &dill::Catalog,
    ) -> Result<Vec<(String, OutboxMessageBoundary)>, InternalError> {
        let state = self.state.lock().unwrap();
        Ok(state
            .latest_message_id_by_producer
            .iter()
            .map(|(producer_name, message_id)| {
                (
                    producer_name.clone(),
                    OutboxMessageBoundary {
                        message_id: *message_id,
                        tx_id: 0, // ignore tx_id for in-memory implementation
                    },
                )
            })
            .collect())
    }

    fn list_consumption_boundaries(
        &self,
        _transactional_catalog: &dill::Catalog,
    ) -> OutboxMessageConsumptionBoundariesStream<'_> {
        let boundaries = {
            let guard = self.state.lock().unwrap();
            guard
                .consumed_boundaries
                .iter()
                .map(|(channel_key, consumed_boundary)| {
                    Ok(OutboxMessageConsumptionBoundary {
                        producer_name: channel_key.producer_name.clone(),
                        consumer_name: channel_key.consumer_name.clone(),
                        last_consumed_message_id: consumed_boundary.message_id,
                        last_tx_id: consumed_boundary.tx_id,
                    })
                })
                .collect::<Vec<_>>()
        };

        Box::pin(tokio_stream::iter(boundaries))
    }

    async fn mark_consumed(
        &self,
        _: &dill::Catalog,
        producer_name: &str,
        consumer_name: &str,
        boundary: OutboxMessageBoundary,
    ) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();

        let channel_key = ChannelKey {
            producer_name: producer_name.to_string(),
            consumer_name: consumer_name.to_string(),
        };

        let entry = state.consumed_boundaries.entry(channel_key).or_default();

        if boundary > *entry {
            *entry = boundary;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
