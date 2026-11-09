// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use database_common::{
    InMemoryPreparedCommit,
    InMemoryTransactionParticipant,
    InMemoryTransactionalMap,
    InMemoryWriteConflictError,
    TransactionId,
    TransactionRef,
};
use internal_error::InternalError;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use messaging_outbox::*;
use wakeup_listener::{HubWakeupListener, WakeupListener};

use super::inmem_outbox_message_log::OutboxMessageLog;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WAKEUP_CHANNEL: &str = "outbox_messages_ready";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Joins the transactions of `InMemoryTransactionManager`: messages and
/// consumption boundaries written in one become visible to others when it
/// commits, and are dropped when it rolls back. Without that manager, every
/// write is committed at once
pub struct InMemoryOutboxMessageBridge {
    log: Mutex<OutboxMessageLog>,
    consumed_boundaries: InMemoryTransactionalMap<ChannelKey, OutboxMessageBoundary>,
    wakeup_hub: Arc<InMemoryWakeupHub>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ChannelKey {
    producer_name: String,
    consumer_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn OutboxMessageBridge)]
#[dill::interface(dyn InMemoryTransactionParticipant)]
impl InMemoryOutboxMessageBridge {
    pub fn new(wakeup_hub: Arc<InMemoryWakeupHub>) -> Self {
        Self {
            log: Mutex::new(OutboxMessageLog::default()),
            consumed_boundaries: InMemoryTransactionalMap::new(),
            wakeup_hub,
        }
    }

    fn transaction_id(transaction_catalog: &dill::Catalog) -> Option<TransactionId> {
        transaction_catalog
            .get_one::<TransactionRef>()
            .ok()
            .map(|transaction_ref| transaction_ref.id())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl OutboxMessageBridge for InMemoryOutboxMessageBridge {
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener> {
        Box::new(HubWakeupListener::new(
            self.wakeup_hub.clone(),
            WAKEUP_CHANNEL,
            OUTBOX_AGENT_NAME,
        ))
    }

    async fn push_message(
        &self,
        transaction_catalog: &dill::Catalog,
        message: NewOutboxMessage,
    ) -> Result<(), InternalError> {
        let transaction_id = Self::transaction_id(transaction_catalog);
        let committed = self.log.lock().unwrap().push(transaction_id, &message);
        if committed {
            self.wakeup_hub.signal(WAKEUP_CHANNEL);
        }
        Ok(())
    }

    async fn get_unprocessed_messages(
        &self,
        transaction_catalog: &dill::Catalog,
        above_boundaries_by_producer: Vec<(String, OutboxMessageBoundary)>,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError> {
        assert!(
            !above_boundaries_by_producer.is_empty(),
            "get_unprocessed_messages requires non-empty boundaries"
        );

        let transaction_id = Self::transaction_id(transaction_catalog);
        let log = self.log.lock().unwrap();

        // The in-memory bridge orders by message ID only and ignores `tx_id`
        Ok(log
            .visible(transaction_id)
            .filter(|message| {
                above_boundaries_by_producer
                    .iter()
                    .any(|(producer_name, above_boundary)| {
                        message.producer_name == *producer_name
                            && message.message_id > above_boundary.message_id
                    })
            })
            .take(batch_size)
            .cloned()
            .collect())
    }

    async fn get_messages_by_producer(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        above_boundary: OutboxMessageBoundary,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError> {
        let transaction_id = Self::transaction_id(transaction_catalog);
        let log = self.log.lock().unwrap();

        Ok(log
            .visible(transaction_id)
            .filter(|message| {
                message.producer_name == producer_name
                    && message.message_id > above_boundary.message_id
            })
            .take(batch_size)
            .cloned()
            .collect())
    }

    async fn get_latest_message_boundaries_by_producer(
        &self,
        transaction_catalog: &dill::Catalog,
    ) -> Result<Vec<(String, OutboxMessageBoundary)>, InternalError> {
        let transaction_id = Self::transaction_id(transaction_catalog);
        let log = self.log.lock().unwrap();

        let mut latest_message_id_by_producer = HashMap::new();
        for message in log.visible(transaction_id) {
            latest_message_id_by_producer.insert(message.producer_name.clone(), message.message_id);
        }

        Ok(latest_message_id_by_producer
            .into_iter()
            .map(|(producer_name, message_id)| {
                (
                    producer_name,
                    OutboxMessageBoundary {
                        message_id,
                        tx_id: 0,
                    },
                )
            })
            .collect())
    }

    async fn list_consumption_boundaries(
        &self,
        transaction_catalog: &dill::Catalog,
    ) -> Result<Vec<OutboxMessageConsumptionBoundary>, InternalError> {
        let transaction_id = Self::transaction_id(transaction_catalog);

        Ok(self.consumed_boundaries.read(transaction_id, |boundaries| {
            boundaries
                .iter()
                .map(
                    |(channel_key, consumed_boundary)| OutboxMessageConsumptionBoundary {
                        producer_name: channel_key.producer_name.clone(),
                        consumer_name: channel_key.consumer_name.clone(),
                        last_consumed_message_id: consumed_boundary.message_id,
                        last_tx_id: consumed_boundary.tx_id,
                    },
                )
                .collect()
        }))
    }

    async fn has_held_back_messages(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_names: &[&str],
    ) -> Result<bool, InternalError> {
        let transaction_id = Self::transaction_id(transaction_catalog);
        Ok(self
            .log
            .lock()
            .unwrap()
            .has_held_back(transaction_id, producer_names))
    }

    async fn mark_consumed(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        consumer_name: &str,
        boundary: OutboxMessageBoundary,
    ) -> Result<(), InternalError> {
        let transaction_id = Self::transaction_id(transaction_catalog);
        let channel_key = ChannelKey {
            producer_name: producer_name.to_string(),
            consumer_name: consumer_name.to_string(),
        };

        // A boundary only moves forward, also over one committed meanwhile, as
        // the `ON CONFLICT ... WHERE` upsert of the SQL backends does
        self.consumed_boundaries
            .write(transaction_id, |boundaries| {
                boundaries.upsert(channel_key, boundary, move |current| {
                    (*current).max(boundary)
                });
            });

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl InMemoryTransactionParticipant for InMemoryOutboxMessageBridge {
    fn begin(&self, transaction_id: TransactionId) {
        self.log.lock().unwrap().begin(transaction_id);
        self.consumed_boundaries.begin(transaction_id);
    }

    fn prepare_commit(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Box<dyn InMemoryPreparedCommit + '_>, InMemoryWriteConflictError> {
        let log = self.log.lock().unwrap();
        let consumed_boundaries = self.consumed_boundaries.prepare_commit(transaction_id)?;

        Ok(Box::new(PreparedOutboxCommit {
            log,
            consumed_boundaries,
            transaction_id,
            wakeup_hub: &self.wakeup_hub,
        }))
    }

    fn rollback(&self, transaction_id: TransactionId) {
        self.log.lock().unwrap().rollback(transaction_id);
        self.consumed_boundaries.rollback(transaction_id);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PreparedOutboxCommit<'a> {
    log: MutexGuard<'a, OutboxMessageLog>,
    consumed_boundaries: Box<dyn InMemoryPreparedCommit + 'a>,
    transaction_id: TransactionId,
    wakeup_hub: &'a InMemoryWakeupHub,
}

impl InMemoryPreparedCommit for PreparedOutboxCommit<'_> {
    fn apply(self: Box<Self>) {
        let Self {
            mut log,
            consumed_boundaries,
            transaction_id,
            wakeup_hub,
        } = *self;

        consumed_boundaries.apply();
        let committed_messages = log.commit(transaction_id);
        drop(log);

        if committed_messages {
            wakeup_hub.signal(WAKEUP_CHANNEL);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
