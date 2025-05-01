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
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_datasets_inmem::{InMemoryDatasetDidSecretKeyRepository, InMemoryDatasetEntryRepository};
use kamu_datasets_repo_tests::dataset_did_secret_key_repo;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_did_secret_key_repo::test_insert_and_locate_dataset_did_secret_keys,
    harness = InMemoryDatasetDidSecretKeyRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryDatasetDidSecretKeyRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryDatasetDidSecretKeyRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder.add::<InMemoryDatasetEntryRepository>();
        catalog_builder.add::<InMemoryDatasetDidSecretKeyRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
