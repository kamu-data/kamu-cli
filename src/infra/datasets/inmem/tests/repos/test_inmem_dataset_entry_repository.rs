// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_datasets_inmem::InMemoryDatasetEntryRepository;
use kamu_datasets_repo_tests::dataset_entry_repo;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_dataset_entry() {
    let harness = InMemoryDatasetEntryRepositoryHarness::new();

    dataset_entry_repo::test_get_dataset_entry(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_dataset_entry_by_name() {
    let harness = InMemoryDatasetEntryRepositoryHarness::new();

    dataset_entry_repo::test_get_dataset_entry_by_name(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_dataset_entries_by_owner_id() {
    let harness = InMemoryDatasetEntryRepositoryHarness::new();

    dataset_entry_repo::test_get_dataset_entries_by_owner_id(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_save_duplicate_dataset_entry() {
    let harness = InMemoryDatasetEntryRepositoryHarness::new();

    dataset_entry_repo::test_try_save_duplicate_dataset_entry(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_save_dataset_entry_with_name_collision() {
    let harness = InMemoryDatasetEntryRepositoryHarness::new();

    dataset_entry_repo::test_try_save_dataset_entry_with_name_collision(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_set_same_dataset_name_for_another_owned_dataset_entry() {
    let harness = InmemDatasetEntryRepositoryHarness::new();

    dataset_entry_repo::test_try_set_same_dataset_name_for_another_owned_dataset_entry(
        &harness.catalog,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_dataset_entry_name() {
    let harness = InMemoryDatasetEntryRepositoryHarness::new();

    dataset_entry_repo::test_update_dataset_entry_name(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_dataset_entry() {
    let harness = InMemoryDatasetEntryRepositoryHarness::new();

    dataset_entry_repo::test_delete_dataset_entry(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryDatasetEntryRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryDatasetEntryRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder.add::<InMemoryDatasetEntryRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
