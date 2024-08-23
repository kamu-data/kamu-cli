// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::MySqlTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_mysql::MySqlAccountRepository;
use sqlx::MySqlPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_no_password_stored,
    harness = MySqlPasswordHashRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_store_couple_account_passwords,
    harness = MySqlPasswordHashRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MySqlPasswordHashRepositoryHarness {
    catalog: Catalog,
}

impl MySqlPasswordHashRepositoryHarness {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
