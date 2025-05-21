// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_accounts_repo_tests::did_secret_key_repository::test_insert_and_locate_did_secret_keys,
    harness = PostgresDidSecretKeyRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresDidSecretKeyRepositoryHarness {
    catalog: dill::Catalog,
}

impl PostgresDidSecretKeyRepositoryHarness {
    pub fn new(pg_pool: sqlx::PgPool) -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add_value(pg_pool);
        b.add::<database_common::PostgresTransactionManager>();
        b.add::<kamu_accounts_postgres::PostgresDidSecretKeyRepository>();

        Self { catalog: b.build() }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
