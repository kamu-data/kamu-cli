// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use database_common::{DatabaseTransactionManager, InMemoryTransactionManager, TransactionRef};
use dill::{Catalog, CatalogBuilder};
use kamu_messaging_outbox_inmem::InMemoryOutboxMessageBridge;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use messaging_outbox::{
    NewOutboxMessage,
    OutboxMessageBoundary,
    OutboxMessageBridge,
    OutboxMessageID,
};
use wakeup_listener::WakeupListenerMetrics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const PRODUCER: &str = "producer";
const CONSUMER: &str = "consumer";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rolled_back_messages_are_dropped() {
    let harness = TransactionalOutboxHarness::new();
    let rolled_back = harness.begin().await;
    harness.push(&rolled_back).await;
    harness.rollback(rolled_back).await;

    let committed = harness.begin().await;
    harness.push(&committed).await;
    harness.commit(committed).await;

    // The rolled-back message leaves a gap, as a database sequence does
    pretty_assertions::assert_eq!(harness.committed_message_ids().await, [2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_later_commit_is_held_back_by_earlier_open_transaction() {
    let harness = TransactionalOutboxHarness::new();
    let earlier = harness.begin().await;
    let later = harness.begin().await;
    harness.push(&earlier).await;
    harness.push(&later).await;

    harness.commit(later).await;

    pretty_assertions::assert_eq!(harness.committed_message_ids().await, [] as [i64; 0]);
    assert!(harness.has_held_back_messages().await);

    harness.commit(earlier).await;

    pretty_assertions::assert_eq!(harness.committed_message_ids().await, [1, 2]);
    assert!(!harness.has_held_back_messages().await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rolled_back_consumption_boundary_is_dropped() {
    let harness = TransactionalOutboxHarness::new();
    let committed = harness.begin().await;
    harness.mark_consumed(&committed, 1).await;
    harness.commit(committed).await;

    let rolled_back = harness.begin().await;
    harness.mark_consumed(&rolled_back, 2).await;
    harness.rollback(rolled_back).await;

    pretty_assertions::assert_eq!(harness.consumed_message_ids().await, [1]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TransactionalOutboxHarness {
    catalog: Catalog,
}

/// A transaction of the harness, with the catalog its writes go through
struct OpenTransaction {
    catalog: Catalog,
    transaction_ref: TransactionRef,
}

impl TransactionalOutboxHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<InMemoryOutboxMessageBridge>();
        b.add::<InMemoryWakeupHub>();
        b.add::<WakeupListenerMetrics>();
        b.add::<InMemoryTransactionManager>();

        Self { catalog: b.build() }
    }

    fn bridge(&self) -> Arc<dyn OutboxMessageBridge> {
        self.catalog.get_one().unwrap()
    }

    fn transaction_manager(&self) -> Arc<dyn DatabaseTransactionManager> {
        self.catalog.get_one().unwrap()
    }

    async fn begin(&self) -> OpenTransaction {
        let transaction_ref = self
            .transaction_manager()
            .make_transaction_ref()
            .await
            .unwrap();
        let mut b = self.catalog.builder_chained();
        transaction_ref.register(&mut b);
        let catalog = b.build();

        OpenTransaction {
            catalog,
            transaction_ref,
        }
    }

    async fn commit(&self, transaction: OpenTransaction) {
        self.transaction_manager()
            .commit_transaction(transaction.transaction_ref)
            .await
            .unwrap();
    }

    async fn rollback(&self, transaction: OpenTransaction) {
        self.transaction_manager()
            .rollback_transaction(transaction.transaction_ref)
            .await
            .unwrap();
    }

    async fn push(&self, transaction: &OpenTransaction) {
        self.bridge()
            .push_message(
                &transaction.catalog,
                NewOutboxMessage {
                    producer_name: PRODUCER.to_string(),
                    content_json: serde_json::json!({}),
                    occurred_on: Utc::now(),
                    version: 1,
                },
            )
            .await
            .unwrap();
    }

    async fn mark_consumed(&self, transaction: &OpenTransaction, message_id: i64) {
        self.bridge()
            .mark_consumed(
                &transaction.catalog,
                PRODUCER,
                CONSUMER,
                OutboxMessageBoundary {
                    message_id: OutboxMessageID::new(message_id),
                    tx_id: 0,
                },
            )
            .await
            .unwrap();
    }

    /// The messages a reader outside every transaction sees
    async fn committed_message_ids(&self) -> Vec<i64> {
        self.message_ids_seen_from(&self.catalog).await
    }

    async fn message_ids_seen_from(&self, catalog: &Catalog) -> Vec<i64> {
        self.bridge()
            .get_messages_by_producer(catalog, PRODUCER, OutboxMessageBoundary::default(), 100)
            .await
            .unwrap()
            .into_iter()
            .map(|message| message.message_id.into_inner())
            .collect()
    }

    async fn has_held_back_messages(&self) -> bool {
        self.bridge()
            .has_held_back_messages(&self.catalog, &[PRODUCER])
            .await
            .unwrap()
    }

    async fn consumed_message_ids(&self) -> Vec<i64> {
        self.bridge()
            .list_consumption_boundaries(&self.catalog)
            .await
            .unwrap()
            .into_iter()
            .map(|boundary| boundary.last_consumed_message_id.into_inner())
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
