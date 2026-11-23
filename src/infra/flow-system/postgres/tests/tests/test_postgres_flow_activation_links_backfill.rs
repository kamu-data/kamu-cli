// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{PostgresTransactionManager, transaction};
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_flow_system::*;
use kamu_flow_system_postgres::{
    PostgresFlowActivationLinkRepository,
    PostgresFlowEventStore,
    PostgresFlowSystemEventBridge,
};
use kamu_flow_system_repo_tests::FlowActivationLinksBackfillScenario;
use kamu_wakeup_listener_postgres::PostgresNotificationHub;
use sqlx::PgPool;
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

    let scenario = harness.write_scenario().await;

    harness.run_backfill().await;

    assert_eq!(
        harness
            .get_downstream_links(scenario.upstream_flow_ids.clone())
            .await,
        scenario.expected_links,
    );
    assert_eq!(harness.count_events_to_project().await, 0);

    harness.write_event_after_history().await;

    assert_eq!(harness.count_events_to_project().await, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowActivationLinksBackfillHarness {
    pg_pool: PgPool,
    catalog: Catalog,
}

impl FlowActivationLinksBackfillHarness {
    fn new(pg_pool: PgPool) -> Self {
        let mut b = CatalogBuilder::new();
        b.add_value(pg_pool.clone());
        b.add::<PostgresTransactionManager>();
        b.add::<PostgresNotificationHub>();
        b.add::<WakeupListenerMetrics>();
        b.add::<PostgresFlowSystemEventBridge>();
        b.add::<PostgresFlowEventStore>();
        b.add::<PostgresFlowActivationLinkRepository>();

        Self {
            pg_pool,
            catalog: b.build(),
        }
    }

    async fn write_scenario(&self) -> FlowActivationLinksBackfillScenario {
        transaction!(|catalog| -> Result<_, InternalError> {
            Ok(FlowActivationLinksBackfillScenario::write(&catalog).await)
        })
        .unwrap()
    }

    async fn write_event_after_history(&self) {
        transaction!(|catalog| -> Result<_, InternalError> {
            FlowActivationLinksBackfillScenario::write_event_after_history(&catalog).await;
            Ok(())
        })
        .unwrap();
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
            let has_held_back_events =
                transaction!(|catalog,
                              flow_system_event_bridge: Arc<dyn FlowSystemEventBridge>|
                 -> Result<_, InternalError> {
                    flow_system_event_bridge
                        .has_held_back_events(&catalog)
                        .await
                })
                .unwrap();
            if !has_held_back_events {
                return;
            }
            assert!(Instant::now() < deadline, "events still held back");
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn get_downstream_links(
        &self,
        upstream_flow_ids: Vec<FlowID>,
    ) -> Vec<FlowActivationLink> {
        transaction!(
            |flow_activation_link_repository: Arc<dyn FlowActivationLinkRepository>|
             -> Result<_, InternalError> {
                flow_activation_link_repository
                    .get_downstream_links(&upstream_flow_ids)
                    .await
            }
        )
        .unwrap()
    }

    /// Events the projector would fetch next, without marking them applied
    async fn count_events_to_project(&self) -> usize {
        self.wait_until_no_held_back_events().await;

        transaction!(|catalog,
                      flow_system_event_bridge: Arc<dyn FlowSystemEventBridge>|
         -> Result<_, InternalError> {
            flow_system_event_bridge
                .fetch_next_batch(&catalog, FLOW_ACTIVATION_LINK_PROJECTOR_NAME, 100)
                .await
        })
        .unwrap()
        .len()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
