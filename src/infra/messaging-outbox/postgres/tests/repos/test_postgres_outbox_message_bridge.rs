// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use database_common::{DatabaseTransactionManager, PostgresTransactionManager};
use database_common_macros::{database_transactional_test, transactional_method};
use dill::{Catalog, CatalogBuilder};
use kamu_messaging_outbox_postgres::PostgresOutboxMessageBridge;
use messaging_outbox::{
    NewOutboxMessage,
    OutboxMessage,
    OutboxMessageBoundary,
    OutboxMessageBridge,
};
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_no_outbox_messages_initially,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_push_messages_from_several_producers,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_push_many_messages_and_read_parts,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_messaging_outbox_repo_tests::test_reading_messages_above_max_with_multiple_producers,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_try_reading_above_max,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_outbox_messages_version,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_consumption_boundaries_monotonic_and_isolated,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_tx_id_priority_over_message_id_in_ordering(pg_pool: PgPool) {
    let harness = PostgresOutboxMessageBridgeHarness::new(pg_pool.clone());
    let transaction_manager = harness
        .catalog
        .get_one::<dyn DatabaseTransactionManager>()
        .unwrap();

    fn make_message(seq: i64) -> NewOutboxMessage {
        NewOutboxMessage {
            producer_name: "A".to_string(),
            content_json: serde_json::json!({"seq": seq}),
            occurred_on: Utc::now(),
            version: 1,
        }
    }

    // Start two transactions and push messages, then commit in different order.
    // Verify that messages are ordered by transaction id, not by message_id.

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

    // Messages from tx1 should be ordered before messages from tx2, even if they
    // have higher message_ids, because tx1 started before tx2 and tx1's messages
    // were pushed before tx2's messages were committed. This is to ensure that
    // messages are consumed in the order they were produced, even if some
    // transactions take longer to commit than others

    let all_messages = harness
        .get_messages_by_producer("A", OutboxMessageBoundary::default(), 10)
        .await
        .unwrap();

    let actual_seq = all_messages
        .iter()
        .map(|message| message.content_json.get("seq").unwrap().as_i64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(actual_seq, vec![1, 3, 2]);

    let second_boundary = OutboxMessageBoundary {
        message_id: all_messages[1].message_id,
        tx_id: all_messages[1].tx_id,
    };

    let tail_messages = harness
        .get_messages_by_producer("A", second_boundary, 10)
        .await
        .unwrap();

    let tail_seq = tail_messages
        .iter()
        .map(|message| message.content_json.get("seq").unwrap().as_i64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(tail_seq, vec![2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresOutboxMessageBridgeHarness {
    catalog: Catalog,
}

impl PostgresOutboxMessageBridgeHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresOutboxMessageBridge>();

        Self {
            catalog: catalog_builder.build(),
        }
    }

    #[transactional_method]
    async fn get_messages_by_producer(
        &self,
        producer_name: &str,
        above_boundary: OutboxMessageBoundary,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, internal_error::InternalError> {
        let outbox_message_bridge = transaction_catalog
            .get_one::<dyn OutboxMessageBridge>()
            .unwrap();

        outbox_message_bridge
            .get_messages_by_producer(
                &transaction_catalog,
                producer_name,
                above_boundary,
                batch_size,
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
