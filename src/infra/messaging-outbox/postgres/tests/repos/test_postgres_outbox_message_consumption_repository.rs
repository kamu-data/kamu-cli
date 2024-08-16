// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{DatabaseTransactionRunner, PostgresTransactionManager};
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_messaging_outbox_postgres::PostgresOutboxMessageConsumptionRepository;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_no_outbox_consumptions_initially(pg_pool: PgPool) {
    let harness = PostgresOutboxMessageConsumptionRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_messaging_outbox_repo_tests::test_no_outbox_consumptions_initially(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_create_consumption(pg_pool: PgPool) {
    let harness = PostgresOutboxMessageConsumptionRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_messaging_outbox_repo_tests::test_create_consumption(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_update_existing_consumption(pg_pool: PgPool) {
    let harness = PostgresOutboxMessageConsumptionRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_messaging_outbox_repo_tests::test_update_existing_consumption(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_cannot_update_consumption_before_creation(pg_pool: PgPool) {
    let harness = PostgresOutboxMessageConsumptionRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_messaging_outbox_repo_tests::test_cannot_update_consumption_before_creation(
                &catalog,
            )
            .await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_multiple_boundaries(pg_pool: PgPool) {
    let harness = PostgresOutboxMessageConsumptionRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_messaging_outbox_repo_tests::test_multiple_boundaries(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresOutboxMessageConsumptionRepositoryHarness {
    catalog: Catalog,
}

impl PostgresOutboxMessageConsumptionRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresOutboxMessageConsumptionRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
