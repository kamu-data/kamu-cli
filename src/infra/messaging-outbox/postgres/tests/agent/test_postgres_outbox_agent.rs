// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use chrono::Utc;
use database_common::{DatabaseTransactionManager, PostgresTransactionManager};
use database_common_macros::transactional_method;
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_messaging_outbox_postgres::PostgresOutboxMessageBridge;
use messaging_outbox::*;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::time::{Duration, Instant};

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
    delivery: MessageDeliveryMechanism::Transactional,
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
        let mut b = CatalogBuilder::new();

        b.add_value(pg_pool);
        b.add::<PostgresTransactionManager>();
        b.add::<PostgresOutboxMessageBridge>();

        b.add::<OutboxAgentMetrics>();
        b.add::<OutboxAgentImpl>();
        b.add_value(OutboxAgentConfig {
            min_debounce_interval: std::time::Duration::from_millis(1),
            max_listening_timeout: std::time::Duration::from_millis(1),
            batch_size: 1,
        });

        b.add::<TestMessageConsumerTxOrder>();
        register_message_dispatcher::<TestMessageTxOrder>(&mut b, TEST_PRODUCER_TX_ORDER);

        let catalog = b.build();
        let outbox_agent = catalog.get_one::<dyn OutboxAgent>().unwrap();

        Self {
            catalog,
            outbox_agent,
        }
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
