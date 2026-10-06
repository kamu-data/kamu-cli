// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{DatabaseTransactionRunner, SqliteTransactionManager};
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_flow_system::*;
use kamu_flow_system_repo_tests::FlowActivationLinksBackfillScenario;
use kamu_flow_system_sqlite::{
    SqliteFlowActivationLinkRepository,
    SqliteFlowEventStore,
    SqliteFlowSystemEventBridge,
};
use kamu_wakeup_listener_sqlite::SqlitePollingHub;
use sqlx::SqlitePool;
use wakeup_listener::{WakeupListenerConfig, WakeupListenerMetrics};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BACKFILL_MIGRATION: &str = include_str!(
    "../../../../../../migrations/sqlite/20261006091721_flow_activation_links_backfill.sql"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_backfill_links_history_and_skips_it_in_projection(sqlite_pool: SqlitePool) {
    let harness = FlowActivationLinksBackfillHarness::new(sqlite_pool);

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
    sqlite_pool: SqlitePool,
    catalog: Catalog,
}

impl FlowActivationLinksBackfillHarness {
    fn new(sqlite_pool: SqlitePool) -> Self {
        let mut b = CatalogBuilder::new();
        b.add_value(sqlite_pool.clone());
        b.add::<SqliteTransactionManager>();
        b.add::<SqlitePollingHub>();
        b.add::<WakeupListenerMetrics>();
        b.add_value(WakeupListenerConfig::local_default());
        b.add::<SqliteFlowSystemEventBridge>();
        b.add::<SqliteFlowEventStore>();
        b.add::<SqliteFlowActivationLinkRepository>();

        Self {
            sqlite_pool,
            catalog: b.build(),
        }
    }

    fn transaction_runner(&self) -> DatabaseTransactionRunner {
        DatabaseTransactionRunner::new(self.catalog.clone())
    }

    async fn write_scenario(&self) -> FlowActivationLinksBackfillScenario {
        self.transaction_runner()
            .transactional(|catalog| async move {
                Ok::<_, InternalError>(FlowActivationLinksBackfillScenario::write(&catalog).await)
            })
            .await
            .unwrap()
    }

    async fn write_event_after_history(&self) {
        self.transaction_runner()
            .transactional(|catalog| async move {
                FlowActivationLinksBackfillScenario::write_event_after_history(&catalog).await;
                Ok::<_, InternalError>(())
            })
            .await
            .unwrap();
    }

    async fn run_backfill(&self) {
        sqlx::raw_sql(BACKFILL_MIGRATION)
            .execute(&self.sqlite_pool)
            .await
            .unwrap();
    }

    async fn get_downstream_links(
        &self,
        upstream_flow_ids: Vec<FlowID>,
    ) -> Vec<FlowActivationLink> {
        self.transaction_runner()
            .transactional_with(
                |flow_activation_link_repository: Arc<
                    dyn FlowActivationLinkRepository,
                >| async move {
                    flow_activation_link_repository
                        .get_downstream_links(&upstream_flow_ids)
                        .await
                },
            )
            .await
            .unwrap()
    }

    /// Events the projector would fetch next, without marking them applied
    async fn count_events_to_project(&self) -> usize {
        self.transaction_runner()
            .transactional(|catalog| async move {
                catalog
                    .get_one::<dyn FlowSystemEventBridge>()
                    .unwrap()
                    .fetch_next_batch(&catalog, FLOW_ACTIVATION_LINK_PROJECTOR_NAME, 100)
                    .await
            })
            .await
            .unwrap()
            .len()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
