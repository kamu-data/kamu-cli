// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::{AccessTokenRepositoryInMemory, AccountRepositoryInMemory};

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_missing_access_token_not_found() {
    let harness = InmemAccessTokenRepositoryHarness::new();
    kamu_accounts_repo_tests::test_missing_access_token_not_found(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_insert_and_locate_access_token() {
    let harness = InmemAccessTokenRepositoryHarness::new();
    kamu_accounts_repo_tests::test_insert_and_locate_access_token(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_insert_and_locate_multiple_access_tokens() {
    let harness = InmemAccessTokenRepositoryHarness::new();
    kamu_accounts_repo_tests::test_insert_and_locate_multiple_access_tokens(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_mark_existing_access_token_revorked() {
    let harness = InmemAccessTokenRepositoryHarness::new();
    kamu_accounts_repo_tests::test_mark_existing_access_token_revorked(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_mark_non_existing_access_token_revorked() {
    let harness = InmemAccessTokenRepositoryHarness::new();
    kamu_accounts_repo_tests::test_mark_non_existing_access_token_revorked(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_find_account_by_active_token_id() {
    let harness = InmemAccessTokenRepositoryHarness::new();
    kamu_accounts_repo_tests::test_find_account_by_active_token_id(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

struct InmemAccessTokenRepositoryHarness {
    catalog: Catalog,
}

impl InmemAccessTokenRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<AccessTokenRepositoryInMemory>();
        catalog_builder.add::<AccountRepositoryInMemory>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}
