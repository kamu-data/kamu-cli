// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use database_common::{DatabaseTransactionManager, PostgresTransactionManager, TransactionRef};
use database_common_macros::transactional_method;
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_messaging_outbox_postgres::PostgresOutboxMessageBridge;
use kamu_wakeup_listener_postgres::PostgresNotificationHub;
use messaging_outbox::*;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, Transaction};
use tokio::time::{Duration, Instant};
use wakeup_listener::{WakeupListenerConfig, WakeupListenerMetrics};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_PRODUCER_TX_ORDER: &str = "TEST-PRODUCER-TX-ORDER";
const TEST_CONSUMER_TX_ORDER: &str = "TestMessageConsumerTxOrder";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestMessageTxOrder {
    seq: i64,
}

impl Message for TestMessageTxOrder {
    fn version() -> u32 {
        1
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct TestMessageConsumerTxOrderState {
    captured_seq: Vec<i64>,
}

struct TestMessageConsumerTxOrder {
    state: Arc<Mutex<TestMessageConsumerTxOrderState>>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<TestMessageTxOrder>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: TEST_CONSUMER_TX_ORDER,
    feeding_producers: &[TEST_PRODUCER_TX_ORDER],
    consumption_mode: MessageConsumptionMode::TransactionalWrapped,
    initial_consumer_boundary: InitialConsumerBoundary::All,
})]
impl TestMessageConsumerTxOrder {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(Default::default())),
        }
    }

    fn consumed_seq(&self) -> Vec<i64> {
        self.state.lock().unwrap().captured_seq.clone()
    }
}

impl MessageConsumer for TestMessageConsumerTxOrder {}

#[async_trait::async_trait]
impl MessageConsumerT<TestMessageTxOrder> for TestMessageConsumerTxOrder {
    async fn consume_message(
        &self,
        _target_catalog: &Catalog,
        message: &TestMessageTxOrder,
    ) -> Result<(), InternalError> {
        self.state.lock().unwrap().captured_seq.push(message.seq);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_PRODUCER_FAILING: &str = "TEST-PRODUCER-FAILING";
const TEST_CONSUMER_FAILING: &str = "TestMessageConsumerFailing";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestMessageFailing {
    seq: i64,
}

impl Message for TestMessageFailing {
    fn version() -> u32 {
        1
    }
}

struct TestMessageConsumerFailing {}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<TestMessageFailing>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: TEST_CONSUMER_FAILING,
    feeding_producers: &[TEST_PRODUCER_FAILING],
    consumption_mode: MessageConsumptionMode::TransactionalWrapped,
    initial_consumer_boundary: InitialConsumerBoundary::All,
})]
impl TestMessageConsumerFailing {
    fn new() -> Self {
        Self {}
    }
}

impl MessageConsumer for TestMessageConsumerFailing {}

#[async_trait::async_trait]
impl MessageConsumerT<TestMessageFailing> for TestMessageConsumerFailing {
    async fn consume_message(
        &self,
        _target_catalog: &Catalog,
        _message: &TestMessageFailing,
    ) -> Result<(), InternalError> {
        Err(InternalError::new("Consumer always fails"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_agent_consumes_messages_in_tx_id_order_across_iterations(pg_pool: PgPool) {
    let harness = PostgresOutboxAgentHarness::new(pg_pool);
    harness.outbox_agent.run_initialization().await.unwrap();

    let transaction_manager = harness
        .catalog
        .get_one::<dyn DatabaseTransactionManager>()
        .unwrap();

    let tx1_ref = transaction_manager.make_transaction_ref().await.unwrap();
    let tx2_ref = transaction_manager.make_transaction_ref().await.unwrap();

    let tx1_catalog = {
        let mut b = harness.catalog.builder_chained();
        tx1_ref.register(&mut b);
        b.build()
    };

    let tx2_catalog = {
        let mut b = harness.catalog.builder_chained();
        tx2_ref.register(&mut b);
        b.build()
    };

    let outbox_tx1 = tx1_catalog.get_one::<dyn OutboxMessageBridge>().unwrap();
    let outbox_tx2 = tx2_catalog.get_one::<dyn OutboxMessageBridge>().unwrap();

    outbox_tx1
        .push_message(&tx1_catalog, make_message(1))
        .await
        .unwrap();

    outbox_tx2
        .push_message(&tx2_catalog, make_message(2))
        .await
        .unwrap();

    drop(outbox_tx2);
    drop(tx2_catalog);

    transaction_manager
        .commit_transaction(tx2_ref)
        .await
        .unwrap();

    outbox_tx1
        .push_message(&tx1_catalog, make_message(3))
        .await
        .unwrap();

    drop(outbox_tx1);
    drop(tx1_catalog);

    transaction_manager
        .commit_transaction(tx1_ref)
        .await
        .unwrap();

    let consumed_seq = harness
        .run_agent_until_consumed_seq(&[1, 3, 2], Duration::from_secs(5))
        .await;

    assert_eq!(consumed_seq, vec![1, 3, 2]);

    let consumed_boundary = harness
        .read_consumed_boundary(TEST_PRODUCER_TX_ORDER, TEST_CONSUMER_TX_ORDER)
        .await
        .unwrap();
    let latest_produced_boundary = harness
        .read_latest_produced_boundary(TEST_PRODUCER_TX_ORDER)
        .await
        .unwrap();

    assert_eq!(consumed_boundary, latest_produced_boundary);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_drain_waits_for_messages_held_back_by_older_transaction(pg_pool: PgPool) {
    let harness = PostgresOutboxAgentHarness::new(pg_pool);
    harness.outbox_agent.run_initialization().await.unwrap();

    // The older transaction writes first, so it holds the lower transaction ID
    let older_tx = harness.begin().await;
    harness.push_message(&older_tx, 1).await;

    let newer_tx = harness.begin().await;
    harness.push_message(&newer_tx, 2).await;
    harness.commit(newer_tx).await;

    // Message 2 is committed but held back: delivering it now would let the
    // boundary pass message 1
    let drain = harness.spawn_drain();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!drain.is_finished());

    harness.commit(older_tx).await;
    drain.await.unwrap();

    assert_eq!(harness.consumed_seq(), [1, 2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_main_loop_delivers_held_back_message_without_a_wakeup(pg_pool: PgPool) {
    // A wakeup would come only from the timeout, far beyond the test's patience
    let harness =
        PostgresOutboxAgentHarness::with_listening_timeout(pg_pool, Duration::from_hours(1));
    harness.outbox_agent.run_initialization().await.unwrap();
    let main_loop = harness.spawn_main_loop();

    // Let the loop finish its catch-up and listen, or the commit below goes unheard
    tokio::time::sleep(Duration::from_millis(200)).await;

    // An older transaction that touches no outbox table: its commit raises no
    // wakeup
    let older_tx = harness.begin_unrelated_writer().await;

    let newer_tx = harness.begin().await;
    harness.push_message(&newer_tx, 1).await;
    harness.commit(newer_tx).await;

    // The commit wakes the loop, which finds the message held back
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(harness.consumed_seq(), Vec::<i64>::new());

    older_tx.commit().await.unwrap();

    let consumed_seq = harness
        .wait_for_consumed_seq(&[1], Duration::from_secs(5))
        .await;
    assert_eq!(consumed_seq, [1]);

    main_loop.abort();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_drain_ignores_held_back_messages_nobody_consumes(pg_pool: PgPool) {
    let harness = PostgresOutboxAgentHarness::new(pg_pool);
    harness.outbox_agent.run_initialization().await.unwrap();

    let older_tx = harness.begin_unrelated_writer().await;

    // Held back, but no consumer would ever take it
    let newer_tx = harness.begin().await;
    harness
        .push_message_from(&newer_tx, "TEST-PRODUCER-WITHOUT-CONSUMERS")
        .await;
    harness.commit(newer_tx).await;

    tokio::time::timeout(Duration::from_secs(2), harness.spawn_drain())
        .await
        .expect("drain must not wait for the older transaction")
        .unwrap();

    older_tx.commit().await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_drain_ignores_held_back_messages_of_failed_consumers(pg_pool: PgPool) {
    let harness = PostgresOutboxAgentHarness::new(pg_pool);
    harness.outbox_agent.run_initialization().await.unwrap();

    // The only consumer of this producer fails, and the agent stops feeding it
    let first_tx = harness.begin().await;
    harness
        .push_message_from(&first_tx, TEST_PRODUCER_FAILING)
        .await;
    harness.commit(first_tx).await;
    harness.outbox_agent.run_while_has_tasks().await.unwrap();

    let older_tx = harness.begin_unrelated_writer().await;

    // Held back, but this agent would not deliver it
    let newer_tx = harness.begin().await;
    harness
        .push_message_from(&newer_tx, TEST_PRODUCER_FAILING)
        .await;
    harness.commit(newer_tx).await;

    tokio::time::timeout(Duration::from_secs(2), harness.spawn_drain())
        .await
        .expect("drain must not wait for the older transaction")
        .unwrap();

    older_tx.commit().await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_message(seq: i64) -> NewOutboxMessage {
    let payload = TestMessageTxOrder { seq };

    NewOutboxMessage {
        producer_name: TEST_PRODUCER_TX_ORDER.to_string(),
        content_json: serde_json::to_value(payload).unwrap(),
        occurred_on: Utc::now(),
        version: TestMessageTxOrder::version(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresOutboxAgentHarness {
    catalog: Catalog,
    outbox_agent: Arc<dyn OutboxAgent>,
}

impl PostgresOutboxAgentHarness {
    fn new(pg_pool: PgPool) -> Self {
        Self::with_listening_timeout(pg_pool, Duration::from_millis(1))
    }

    fn with_listening_timeout(pg_pool: PgPool, max_listening_timeout: Duration) -> Self {
        let mut b = CatalogBuilder::new();

        b.add_value(pg_pool);
        b.add::<PostgresTransactionManager>();
        b.add::<PostgresOutboxMessageBridge>();
        b.add::<PostgresNotificationHub>();
        b.add::<WakeupListenerMetrics>();

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

        b.add::<TestMessageConsumerTxOrder>();
        register_message_dispatcher::<TestMessageTxOrder>(&mut b, TEST_PRODUCER_TX_ORDER);

        b.add::<TestMessageConsumerFailing>();
        register_message_dispatcher::<TestMessageFailing>(&mut b, TEST_PRODUCER_FAILING);

        let catalog = b.build();
        let outbox_agent = catalog.get_one::<dyn OutboxAgent>().unwrap();

        Self {
            catalog,
            outbox_agent,
        }
    }

    async fn begin(&self) -> TransactionRef {
        self.catalog
            .get_one::<dyn DatabaseTransactionManager>()
            .unwrap()
            .make_transaction_ref()
            .await
            .unwrap()
    }

    async fn commit(&self, transaction_ref: TransactionRef) {
        self.catalog
            .get_one::<dyn DatabaseTransactionManager>()
            .unwrap()
            .commit_transaction(transaction_ref)
            .await
            .unwrap();
    }

    async fn push_message(&self, transaction_ref: &TransactionRef, seq: i64) {
        let mut b = self.catalog.builder_chained();
        transaction_ref.register(&mut b);
        let transaction_catalog = b.build();

        transaction_catalog
            .get_one::<dyn OutboxMessageBridge>()
            .unwrap()
            .push_message(&transaction_catalog, make_message(seq))
            .await
            .unwrap();
    }

    async fn push_message_from(&self, transaction_ref: &TransactionRef, producer_name: &str) {
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
                    ..make_message(0)
                },
            )
            .await
            .unwrap();
    }

    /// A transaction holding a transaction ID, with no outbox writes
    async fn begin_unrelated_writer(&self) -> Transaction<'static, Postgres> {
        let mut transaction = self
            .catalog
            .get_one::<PgPool>()
            .unwrap()
            .begin()
            .await
            .unwrap();
        sqlx::query("SELECT pg_current_xact_id()")
            .execute(&mut *transaction)
            .await
            .unwrap();
        transaction
    }

    fn spawn_main_loop(&self) -> tokio::task::JoinHandle<()> {
        let outbox_agent = self.outbox_agent.clone();
        tokio::spawn(async move { outbox_agent.run().await.unwrap() })
    }

    async fn wait_for_consumed_seq(&self, expected_seq: &[i64], timeout: Duration) -> Vec<i64> {
        let deadline = Instant::now() + timeout;
        loop {
            let actual = self.consumed_seq();
            if actual == expected_seq || Instant::now() >= deadline {
                return actual;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn spawn_drain(&self) -> tokio::task::JoinHandle<()> {
        let outbox_agent = self.outbox_agent.clone();
        tokio::spawn(async move { outbox_agent.run_while_has_tasks().await.unwrap() })
    }

    fn consumed_seq(&self) -> Vec<i64> {
        self.catalog
            .get_one::<TestMessageConsumerTxOrder>()
            .unwrap()
            .consumed_seq()
    }

    async fn run_agent_until_consumed_seq(
        &self,
        expected_seq: &[i64],
        timeout: Duration,
    ) -> Vec<i64> {
        let deadline = Instant::now() + timeout;

        loop {
            self.outbox_agent.run_while_has_tasks().await.unwrap();

            let consumer = self
                .catalog
                .get_one::<TestMessageConsumerTxOrder>()
                .unwrap();
            let actual = consumer.consumed_seq();

            if actual == expected_seq {
                return actual;
            }

            if Instant::now() >= deadline {
                let consumed_boundary = self
                    .read_consumed_boundary(TEST_PRODUCER_TX_ORDER, TEST_CONSUMER_TX_ORDER)
                    .await
                    .unwrap();
                let latest_produced_boundary = self
                    .read_latest_produced_boundary_maybe(TEST_PRODUCER_TX_ORDER)
                    .await
                    .unwrap();

                panic!(
                    "Timed out waiting for expected consumed sequence. expected={expected_seq:?}, \
                     actual={actual:?}, consumed_boundary={consumed_boundary:?}, \
                     latest_produced_boundary={latest_produced_boundary:?}"
                );
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[transactional_method]
    async fn read_consumed_boundary(
        &self,
        producer_name: &str,
        consumer_name: &str,
    ) -> Result<OutboxMessageBoundary, InternalError> {
        let outbox_message_bridge = transaction_catalog
            .get_one::<dyn OutboxMessageBridge>()
            .unwrap();

        let boundary = outbox_message_bridge
            .list_consumption_boundaries(&transaction_catalog)
            .await
            .unwrap()
            .into_iter()
            .find(|boundary| {
                boundary.producer_name == producer_name && boundary.consumer_name == consumer_name
            })
            .map(|boundary| boundary.boundary())
            .unwrap();

        Ok(boundary)
    }

    async fn read_latest_produced_boundary(
        &self,
        producer_name: &str,
    ) -> Result<OutboxMessageBoundary, InternalError> {
        Ok(self
            .read_latest_produced_boundary_maybe(producer_name)
            .await?
            .unwrap())
    }

    #[transactional_method]
    async fn read_latest_produced_boundary_maybe(
        &self,
        producer_name: &str,
    ) -> Result<Option<OutboxMessageBoundary>, InternalError> {
        let outbox_message_bridge = transaction_catalog
            .get_one::<dyn OutboxMessageBridge>()
            .unwrap();

        let boundary = outbox_message_bridge
            .get_latest_message_boundaries_by_producer(&transaction_catalog)
            .await
            .unwrap()
            .into_iter()
            .find(|(name, _)| name == producer_name)
            .map(|(_, boundary)| boundary);

        Ok(boundary)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
