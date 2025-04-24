// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::*;
use kamu_accounts_postgres::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_accounts_repo_tests::oauth_device_code_repository::test_save_device_code,
    harness = PostgresOAuthDeviceCodeRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_accounts_repo_tests::oauth_device_code_repository::test_update_device_token_with_token_params_part,
    harness = PostgresOAuthDeviceCodeRepositoryHarness
);
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_accounts_repo_tests::oauth_device_code_repository::test_find_device_token_by_device_code,
    harness = PostgresOAuthDeviceCodeRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_accounts_repo_tests::oauth_device_code_repository::test_cleanup_expired_device_codes,
    harness = PostgresOAuthDeviceCodeRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresOAuthDeviceCodeRepositoryHarness {
    catalog: dill::Catalog,
}

impl PostgresOAuthDeviceCodeRepositoryHarness {
    pub fn new(pool: sqlx::PgPool) -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add_value(pool);
        b.add::<database_common::PostgresTransactionManager>();
        b.add::<PostgresOAuthDeviceCodeRepository>();
        b.add::<PostgresAccountRepository>();

        Self { catalog: b.build() }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
