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
use kamu_accounts_mysql::{MySqlAccessTokenRepository, MySqlAccountRepository};
use sqlx::MySqlPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_missing_access_token_not_found,
    harness = MySqlAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_insert_and_locate_access_token,
    harness = MySqlAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_insert_and_locate_multiple_access_tokens,
    harness = MySqlAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_mark_existing_access_token_revoked,
    harness = MySqlAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_create_duplicate_active_access_token,
    harness = MySqlAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_create_duplicate_access_token_error,
    harness = MySqlAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_mark_non_existing_access_token_revoked,
    harness = MySqlAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_find_account_by_active_token_id,
    harness = MySqlAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MySqlAccessTokenRepositoryHarness {
    catalog: Catalog,
}

impl MySqlAccessTokenRepositoryHarness {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(mysql_pool);
        catalog_builder.add::<MySqlTransactionManager>();
        catalog_builder.add::<MySqlAccessTokenRepository>();
        catalog_builder.add::<MySqlAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
