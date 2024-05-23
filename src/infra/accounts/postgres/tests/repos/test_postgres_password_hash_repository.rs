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
use kamu_accounts_postgres::PostgresAccountRepository;
use sqlx::PgPool;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_no_password_stored(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional([&harness.catalog], |[catalog]| async move {
        kamu_accounts_repo_tests::test_no_password_stored(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_store_couple_account_passwords(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional([&harness.catalog], |[catalog]| async move {
        kamu_accounts_repo_tests::test_store_couple_account_passwords(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

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
        catalog_builder.add::<PostgresAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
