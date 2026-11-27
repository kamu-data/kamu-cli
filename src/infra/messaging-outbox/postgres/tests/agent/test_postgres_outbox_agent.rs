// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use background_agent_host::CancellationToken;
use database_common::PostgresTransactionManager;
use dill::CatalogBuilder;
use kamu_messaging_outbox_postgres::PostgresOutboxMessageBridge;
use kamu_messaging_outbox_repo_tests::OutboxAgentHarness;
use kamu_wakeup_listener_postgres::PostgresNotificationHub;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::time::{Duration, Instant};
use wakeup_listener::WakeupListenerMetrics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Postgres only: SQLite serializes writers and the in-memory bridge orders by
// message ID alone, so only Postgres orders messages by transaction ID

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_agent_consumes_messages_in_tx_id_order_across_iterations(pg_pool: PgPool) {
    let harness = PostgresOutboxAgentHarness::new(pg_pool).await;

    let tx1 = harness.begin().await;
    let tx2 = harness.begin().await;

    harness.push_message(&tx1, 1).await;
    harness.push_message(&tx2, 2).await;
    harness.commit(tx2).await;

    harness.push_message(&tx1, 3).await;
    harness.commit(tx1).await;

    let consumed_seq = harness
        .run_agent_until_consumed_seq(&[1, 3, 2], Duration::from_secs(5))
        .await;

    assert_eq!(consumed_seq, vec![1, 3, 2]);
    assert_eq!(
        Some(harness.consumed_boundary().await),
        harness.latest_produced_boundary().await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Postgres only: an in-memory transaction holds messages back only by pushing
// one, and then its commit wakes the agent

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_main_loop_delivers_held_back_message_without_a_wakeup(pg_pool: PgPool) {
    // A wakeup would come only from the timeout, far beyond the test's patience
    let harness =
        PostgresOutboxAgentHarness::with_listening_timeout(pg_pool, Duration::from_hours(1)).await;
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
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(OutboxAgentHarness, agent)]
struct PostgresOutboxAgentHarness {
    agent: OutboxAgentHarness,
    pg_pool: PgPool,
}

impl PostgresOutboxAgentHarness {
    async fn new(pg_pool: PgPool) -> Self {
        Self::with_listening_timeout(pg_pool, Duration::from_millis(1)).await
    }

    async fn with_listening_timeout(pg_pool: PgPool, max_listening_timeout: Duration) -> Self {
        let mut b = CatalogBuilder::new();
        b.add_value(pg_pool.clone());
        b.add::<PostgresTransactionManager>();
        b.add::<PostgresOutboxMessageBridge>();
        b.add::<PostgresNotificationHub>();
        b.add::<WakeupListenerMetrics>();

        let agent =
            OutboxAgentHarness::with_listening_timeout(&b.build(), max_listening_timeout).await;

        Self { agent, pg_pool }
    }

    /// A transaction holding a transaction ID, with no outbox writes
    async fn begin_unrelated_writer(&self) -> Transaction<'static, Postgres> {
        let mut transaction = self.pg_pool.begin().await.unwrap();
        sqlx::query("SELECT pg_current_xact_id()")
            .execute(&mut *transaction)
            .await
            .unwrap();
        transaction
    }

    fn spawn_main_loop(&self) -> tokio::task::JoinHandle<()> {
        let outbox_agent = self.outbox_agent();
        tokio::spawn(async move { outbox_agent.run(CancellationToken::new()).await.unwrap() })
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

    async fn run_agent_until_consumed_seq(
        &self,
        expected_seq: &[i64],
        timeout: Duration,
    ) -> Vec<i64> {
        let deadline = Instant::now() + timeout;

        loop {
            self.drain().await;

            let actual = self.consumed_seq();
            if actual == expected_seq {
                return actual;
            }

            if Instant::now() >= deadline {
                let consumed_boundary = self.consumed_boundary().await;
                let latest_produced_boundary = self.latest_produced_boundary().await;

                panic!(
                    "Timed out waiting for expected consumed sequence. expected={expected_seq:?}, \
                     actual={actual:?}, consumed_boundary={consumed_boundary:?}, \
                     latest_produced_boundary={latest_produced_boundary:?}"
                );
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
