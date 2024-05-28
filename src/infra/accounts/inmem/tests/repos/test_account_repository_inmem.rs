// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::AccountRepositoryInMemory;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_missing_account_not_found() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_missing_account_not_found(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_insert_and_locate_cli_account() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_insert_and_locate_password_account(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_insert_and_locate_github_account() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_insert_and_locate_github_account(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_insert_and_locate_multiple_github_account() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_insert_and_locate_multiple_github_account(&harness.catalog)
        .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_insert_and_locate_account_without_email() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_insert_and_locate_account_without_email(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_duplicate_password_account_id() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_duplicate_password_account_id(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_duplicate_password_account_email() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_duplicate_password_account_email(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_duplicate_github_account_id() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_duplicate_github_account_id(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_duplicate_github_account_name() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_duplicate_github_account_name(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_duplicate_github_account_provider_identity() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_duplicate_github_account_provider_identity(&harness.catalog)
        .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_duplicate_github_account_email() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_duplicate_github_account_email(&harness.catalog).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

struct InmemAccountRepositoryHarness {
    catalog: Catalog,
}

impl InmemAccountRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<AccountRepositoryInMemory>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
