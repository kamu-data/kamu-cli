// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{run_transactional, SqliteTransactionManager};
use dill::{Catalog, CatalogBuilder};
use kamu_flow_system_sqlite::FlowSystemEventStoreSqlite;
use sqlx::SqlitePool;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_event_store_empty(sqlite_pool: SqlitePool) {
    let harness = SqliteAccountRepositoryHarness::new(sqlite_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_flow_system_repo_tests::test_event_store_empty(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_event_store_get_streams(sqlite_pool: SqlitePool) {
    let harness = SqliteAccountRepositoryHarness::new(sqlite_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_flow_system_repo_tests::test_event_store_get_streams(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_event_store_get_events_with_windowing(sqlite_pool: SqlitePool) {
    let harness = SqliteAccountRepositoryHarness::new(sqlite_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_flow_system_repo_tests::test_event_store_get_events_with_windowing(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////
// Harness
/////////////////////////////////////////////////////////////////////////////////////////

struct SqliteAccountRepositoryHarness {
    catalog: Catalog,
}

impl SqliteAccountRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<FlowSystemEventStoreSqlite>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}
/////////////////////////////////////////////////////////////////////////////////////////
