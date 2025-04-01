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
use kamu_datasets_inmem::{InMemoryDatasetEntryRepository, InMemoryDatasetStatisticsRepository};
use kamu_datasets_repo_tests::dataset_statistics_repo;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_statistics_repo::test_set_and_get_statistics,
    harness = InMemoryDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_statistics_repo::test_overwrite_statistics,
    harness = InMemoryDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_statistics_repo::test_multiple_datasets_statistics,
    harness = InMemoryDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_statistics_repo::test_remove_dataset_entry_removes_statistics,
    harness = InMemoryDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryDatasetStatisticsRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryDatasetStatisticsRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder.add::<InMemoryDatasetEntryRepository>();
        catalog_builder.add::<InMemoryDatasetStatisticsRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
