// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, CatalogBuilder};
use kamu_dataset_env_vars_inmem::DatasetEnvVarRepositoryInMemory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_missing_dataset_env_var_not_found() {
    let harness = InmemDatasetEnvVarRepositoryHarness::new();
    kamu_dataset_env_vars_repo_tests::test_missing_dataset_env_var_not_found(&harness.catalog)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_insert_and_get_dataset_env_var() {
    let harness = InmemDatasetEnvVarRepositoryHarness::new();
    kamu_dataset_env_vars_repo_tests::test_insert_and_get_dataset_env_var(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_insert_and_get_multiple_dataset_env_vars() {
    let harness = InmemDatasetEnvVarRepositoryHarness::new();
    kamu_dataset_env_vars_repo_tests::test_insert_and_get_multiple_dataset_env_vars(
        &harness.catalog,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_dataset_env_vars() {
    let harness = InmemDatasetEnvVarRepositoryHarness::new();
    kamu_dataset_env_vars_repo_tests::test_delete_dataset_env_vars(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify_dataset_env_vars() {
    let harness = InmemDatasetEnvVarRepositoryHarness::new();
    kamu_dataset_env_vars_repo_tests::test_modify_dataset_env_vars(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InmemDatasetEnvVarRepositoryHarness {
    catalog: Catalog,
}

impl InmemDatasetEnvVarRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<DatasetEnvVarRepositoryInMemory>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}
