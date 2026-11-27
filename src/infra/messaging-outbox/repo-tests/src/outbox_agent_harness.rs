// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use database_common::testing::in_committed_transaction;
use database_common::{DatabaseTransactionManager, TransactionRef};
use dill::{Catalog, CatalogBuilder};
use messaging_outbox::{
    Message,
    NewOutboxMessage,
    OutboxAgent,
    OutboxAgentConfig,
    OutboxAgentImpl,
    OutboxAgentMetrics,
    OutboxMessageBoundary,
    OutboxMessageBridge,
    register_message_dispatcher,
};
use serde::Serialize;
use tokio::task::JoinHandle;
use wakeup_listener::WakeupListenerConfig;

use crate::outbox_agent_test_consumers::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An initialized `OutboxAgentImpl` delivering one message per iteration to
/// a recording consumer and a failing one, over the storage of the catalog it
/// is built on
pub struct OutboxAgentHarness {
    catalog: Catalog,
    outbox_agent: Arc<dyn OutboxAgent>,
}

impl OutboxAgentHarness {
    /// `storage_catalog` provides the outbox bridge, its wakeup hub with
    /// `WakeupListenerMetrics`, and a transaction manager
    pub async fn new(storage_catalog: &Catalog) -> Self {
        Self::with_listening_timeout(storage_catalog, Duration::from_millis(1)).await
    }

    pub async fn with_listening_timeout(
        storage_catalog: &Catalog,
        max_listening_timeout: Duration,
    ) -> Self {
        let mut b = CatalogBuilder::new_chained(storage_catalog);

        b.add::<OutboxAgentMetrics>();
        b.add::<OutboxAgentImpl>();
        b.add_value(OutboxAgentConfig {
            batch_size: NonZeroUsize::MIN,
            ..OutboxAgentConfig::local_default()
        });
        b.add_value(WakeupListenerConfig {
            min_debounce_interval: Duration::from_millis(1),
            max_listening_timeout,
        });

        b.add::<TestMessageConsumer>();
        register_message_dispatcher::<TestMessage>(&mut b, TEST_PRODUCER);

        b.add::<FailingTestMessageConsumer>();
        register_message_dispatcher::<FailingTestMessage>(&mut b, TEST_PRODUCER_FAILING);

        let catalog = b.build();
        let outbox_agent = catalog.get_one::<dyn OutboxAgent>().unwrap();
        outbox_agent.run_initialization().await.unwrap();

        Self {
            catalog,
            outbox_agent,
        }
    }

    pub fn outbox_agent(&self) -> Arc<dyn OutboxAgent> {
        self.outbox_agent.clone()
    }

    /// Delivers until no committed message is left, as `run_while_has_tasks`
    pub async fn drain(&self) {
        self.outbox_agent.run_while_has_tasks().await.unwrap();
    }

    pub fn spawn_drain(&self) -> JoinHandle<()> {
        let outbox_agent = self.outbox_agent.clone();
        tokio::spawn(async move {
            outbox_agent.run_while_has_tasks().await.unwrap();
        })
    }

    pub async fn begin(&self) -> TransactionRef {
        self.transaction_manager()
            .make_transaction_ref()
            .await
            .unwrap()
    }

    /// Opens a transaction that holds back the messages committed after it on
    /// every storage: it pushes a message nobody consumes, which takes the
    /// next message ID, and on Postgres a transaction ID
    pub async fn begin_older_transaction(&self) -> TransactionRef {
        let transaction_ref = self.begin().await;
        self.push_unconsumed_message(&transaction_ref).await;
        transaction_ref
    }

    pub async fn commit(&self, transaction_ref: TransactionRef) {
        self.transaction_manager()
            .commit_transaction(transaction_ref)
            .await
            .unwrap();
    }

    /// Pushes a message for the recording consumer
    pub async fn push_message(&self, transaction_ref: &TransactionRef, seq: i64) {
        self.push(transaction_ref, TEST_PRODUCER, &TestMessage { seq })
            .await;
    }

    /// Pushes a message for the consumer that always fails
    pub async fn push_failing_message(&self, transaction_ref: &TransactionRef) {
        self.push(
            transaction_ref,
            TEST_PRODUCER_FAILING,
            &FailingTestMessage {},
        )
        .await;
    }

    pub async fn push_unconsumed_message(&self, transaction_ref: &TransactionRef) {
        self.push(
            transaction_ref,
            TEST_PRODUCER_WITHOUT_CONSUMERS,
            &TestMessage { seq: 0 },
        )
        .await;
    }

    /// The sequence numbers the recording consumer took, in order
    pub fn consumed_seq(&self) -> Vec<i64> {
        self.catalog
            .get_one::<TestMessageConsumer>()
            .unwrap()
            .consumed_seq()
    }

    /// The boundary the recording consumer has consumed up to
    pub async fn consumed_boundary(&self) -> OutboxMessageBoundary {
        in_committed_transaction(&self.catalog, |catalog| async move {
            catalog
                .get_one::<dyn OutboxMessageBridge>()
                .unwrap()
                .list_consumption_boundaries(&catalog)
                .await
                .unwrap()
                .into_iter()
                .find(|boundary| {
                    boundary.producer_name == TEST_PRODUCER
                        && boundary.consumer_name == TEST_CONSUMER
                })
                .unwrap()
                .boundary()
        })
        .await
    }

    /// The boundary of the latest message for the recording consumer, if any
    pub async fn latest_produced_boundary(&self) -> Option<OutboxMessageBoundary> {
        in_committed_transaction(&self.catalog, |catalog| async move {
            catalog
                .get_one::<dyn OutboxMessageBridge>()
                .unwrap()
                .get_latest_message_boundaries_by_producer(&catalog)
                .await
                .unwrap()
                .into_iter()
                .find(|(producer_name, _)| producer_name == TEST_PRODUCER)
                .map(|(_, boundary)| boundary)
        })
        .await
    }

    fn transaction_manager(&self) -> Arc<dyn DatabaseTransactionManager> {
        self.catalog
            .get_one::<dyn DatabaseTransactionManager>()
            .unwrap()
    }

    async fn push<M: Message + Serialize>(
        &self,
        transaction_ref: &TransactionRef,
        producer_name: &str,
        message: &M,
    ) {
        let mut b = self.catalog.builder_chained();
        transaction_ref.register(&mut b);
        let transaction_catalog = b.build();

        transaction_catalog
            .get_one::<dyn OutboxMessageBridge>()
            .unwrap()
            .push_message(
                &transaction_catalog,
                NewOutboxMessage {
                    producer_name: producer_name.to_string(),
                    content_json: serde_json::to_value(message).unwrap(),
                    occurred_on: Utc::now(),
                    version: M::version(),
                },
            )
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
