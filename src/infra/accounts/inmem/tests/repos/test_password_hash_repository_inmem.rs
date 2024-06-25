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

////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_password_stored() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_no_password_stored(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_store_couple_account_passwords() {
    let harness = InmemAccountRepositoryHarness::new();
    kamu_accounts_repo_tests::test_store_couple_account_passwords(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
