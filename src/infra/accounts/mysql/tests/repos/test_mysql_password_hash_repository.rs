// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{DatabaseTransactionRunner, MySqlTransactionManager};
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_mysql::MySqlAccountRepository;
use sqlx::MySqlPool;

///////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_no_password_stored(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    <DatabaseTransactionRunner>::run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_no_password_stored(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

///////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_store_couple_account_passwords(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    <DatabaseTransactionRunner>::run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_store_couple_account_passwords(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

///////////////////////////////////////////////////////////////////////////////

struct MySqlAccountRepositoryHarness {
    catalog: Catalog,
}

impl MySqlAccountRepositoryHarness {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        // Initialize catalog with predefined MySql pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(mysql_pool);
        catalog_builder.add::<MySqlTransactionManager>();
        catalog_builder.add::<MySqlAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
