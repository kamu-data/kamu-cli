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
    fixture = kamu_accounts_repo_tests::test_missing_account_not_found,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_insert_and_locate_password_account,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_insert_and_locate_github_account,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_insert_and_locate_multiple_github_account,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_insert_and_locate_account_without_email,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_duplicate_password_account_id,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_duplicate_password_account_email,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_duplicate_github_account_id,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_duplicate_github_account_name,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_duplicate_github_account_provider_identity,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_duplicate_github_account_email,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_update_email_success,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_update_email_errors,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_update_account_success,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_update_account_not_found,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_update_account_duplicate_name,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_update_account_duplicate_email,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = kamu_accounts_repo_tests::test_update_account_duplicate_provider_identity,
    harness = MySqlAccountRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
