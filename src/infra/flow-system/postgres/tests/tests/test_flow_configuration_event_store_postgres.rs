// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{run_transactional, PostgresTransactionManager};
use dill::{Catalog, CatalogBuilder};
use kamu_flow_system_postgres::FlowConfigurationEventStorePostgres;
use sqlx::PgPool;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_event_store_empty(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_flow_system_repo_tests::test_event_store_empty(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_event_store_get_streams(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_flow_system_repo_tests::test_event_store_get_streams(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_event_store_get_events_with_windowing(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

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

struct PostgresAccountRepositoryHarness {
    catalog: Catalog,
}

impl PostgresAccountRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<FlowConfigurationEventStorePostgres>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
