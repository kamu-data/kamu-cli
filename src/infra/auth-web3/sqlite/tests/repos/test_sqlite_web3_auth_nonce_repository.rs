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
    storage = sqlite,
    fixture = kamu_auth_web3_repo_tests::test_set_and_get_nonce,
    harness = SqliteWeb3AuthNonceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_auth_web3_repo_tests::test_cleanup_expired_nonces,
    harness = SqliteWeb3AuthNonceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteWeb3AuthNonceRepositoryHarness {
    catalog: dill::Catalog,
}

impl SqliteWeb3AuthNonceRepositoryHarness {
    pub fn new(pg_pool: sqlx::SqlitePool) -> Self {
        let mut b = dill::CatalogBuilder::new();

        b.add_value(pg_pool);
        b.add::<database_common::SqliteTransactionManager>();
        b.add::<kamu_auth_web3_sqlite::SqliteWeb3AuthNonceRepository>();

        Self { catalog: b.build() }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
