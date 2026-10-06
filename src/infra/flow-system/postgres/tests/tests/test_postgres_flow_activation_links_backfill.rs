// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::TransactionRefT;
use dill::{Catalog, CatalogBuilder};
use kamu_flow_system::*;
use kamu_flow_system_postgres::{
    PostgresFlowActivationLinkRepository,
    PostgresFlowEventStore,
    PostgresFlowSystemEventBridge,
};
use kamu_flow_system_repo_tests::FlowActivationLinksBackfillScenario;
use kamu_wakeup_listener_postgres::PostgresNotificationHub;
use sqlx::{PgPool, Postgres};
use tokio::time::{Duration, Instant, sleep};
use wakeup_listener::WakeupListenerMetrics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BACKFILL_MIGRATION: &str = include_str!(
    "../../../../../../migrations/postgres/20261006091721_flow_activation_links_backfill.sql"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_backfill_links_history_and_skips_it_in_projection(pg_pool: PgPool) {
    let harness = FlowActivationLinksBackfillHarness::new(pg_pool);

    let transaction = harness.begin();
    let scenario =
        FlowActivationLinksBackfillScenario::write(&harness.catalog_for(&transaction)).await;
    FlowActivationLinksBackfillHarness::commit(transaction).await;

    harness.run_backfill().await;

    assert_eq!(
        harness
            .get_downstream_links(&scenario.upstream_flow_ids)
            .await,
        scenario.expected_links,
    );
    assert_eq!(harness.count_events_to_project().await, 0);

    let transaction = harness.begin();
    FlowActivationLinksBackfillScenario::write_event_after_history(
        &harness.catalog_for(&transaction),
    )
    .await;
    FlowActivationLinksBackfillHarness::commit(transaction).await;

    assert_eq!(harness.count_events_to_project().await, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowActivationLinksBackfillHarness {
    pg_pool: PgPool,
    base_catalog: Catalog,
}

impl FlowActivationLinksBackfillHarness {
    fn new(pg_pool: PgPool) -> Self {
        let mut b = CatalogBuilder::new();
        b.add_value(pg_pool.clone());
        b.add::<PostgresNotificationHub>();
        b.add::<WakeupListenerMetrics>();
        b.add::<PostgresFlowSystemEventBridge>();
        b.add::<PostgresFlowEventStore>();
        b.add::<PostgresFlowActivationLinkRepository>();

        Self {
            pg_pool,
            base_catalog: b.build(),
        }
    }

    fn begin(&self) -> TransactionRefT<Postgres> {
        TransactionRefT::<Postgres>::new(self.pg_pool.clone())
    }

    async fn commit(transaction: TransactionRefT<Postgres>) {
        transaction
            .into_inner_db_transaction()
            .unwrap()
            .commit()
            .await
            .unwrap();
    }

    fn catalog_for(&self, transaction: &TransactionRefT<Postgres>) -> Catalog {
        let mut b = CatalogBuilder::new_chained(&self.base_catalog);
        transaction.clone().into_erased().register(&mut b);
        b.build()
    }

    async fn run_backfill(&self) {
        sqlx::raw_sql(BACKFILL_MIGRATION)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }

    /// Transactions of tests sharing the server hold back fresh events until
    /// they finish
    async fn wait_until_no_held_back_events(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let transaction = self.begin();
            let catalog = self.catalog_for(&transaction);
            let has_held_back_events = catalog
                .get_one::<dyn FlowSystemEventBridge>()
                .unwrap()
                .has_held_back_events(&catalog)
                .await
                .unwrap();
            if !has_held_back_events {
                return;
            }
            assert!(Instant::now() < deadline, "events still held back");
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn get_downstream_links(&self, upstream_flow_ids: &[FlowID]) -> Vec<FlowActivationLink> {
        let transaction = self.begin();
        self.catalog_for(&transaction)
            .get_one::<dyn FlowActivationLinkRepository>()
            .unwrap()
            .get_downstream_links(upstream_flow_ids)
            .await
            .unwrap()
    }

    /// Events the projector would fetch next, without marking them applied
    async fn count_events_to_project(&self) -> usize {
        self.wait_until_no_held_back_events().await;

        let transaction = self.begin();
        let catalog = self.catalog_for(&transaction);
        catalog
            .get_one::<dyn FlowSystemEventBridge>()
            .unwrap()
            .fetch_next_batch(&catalog, FLOW_ACTIVATION_LINK_PROJECTOR_NAME, 100)
            .await
            .unwrap()
            .len()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
