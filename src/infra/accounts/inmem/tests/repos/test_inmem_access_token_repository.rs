// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::{InMemoryAccessTokenRepository, InMemoryAccountRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_missing_access_token_not_found,
    harness = InMemoryAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_insert_and_locate_access_token,
    harness = InMemoryAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_insert_and_locate_multiple_access_tokens,
    harness = InMemoryAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_mark_existing_access_token_revoked,
    harness = InMemoryAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_create_duplicate_active_access_token,
    harness = InMemoryAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_create_duplicate_access_token_err,
    harness = InMemoryAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_mark_non_existing_access_token_revoked,
    harness = InMemoryAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_find_account_by_active_token_id,
    harness = InMemoryAccessTokenRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryAccessTokenRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryAccessTokenRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryAccessTokenRepository>();
        catalog_builder.add::<InMemoryAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
