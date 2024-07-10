// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, CatalogBuilder};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_get_properties_from_nonexistent_entity() {
    let harness = InmemRebacRepositoryHarness::new();

    kamu_auth_rebac_repo_tests::test_try_get_properties_from_nonexistent_entity(&harness.catalog)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_property() {
    let harness = InmemRebacRepositoryHarness::new();

    kamu_auth_rebac_repo_tests::test_set_property(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_delete_property_from_nonexistent_entity() {
    let harness = InmemRebacRepositoryHarness::new();

    kamu_auth_rebac_repo_tests::test_try_delete_property_from_nonexistent_entity(&harness.catalog)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_delete_nonexistent_property_from_entity() {
    let harness = InmemRebacRepositoryHarness::new();

    kamu_auth_rebac_repo_tests::test_try_delete_nonexistent_property_from_entity(&harness.catalog)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_property_from_entity() {
    let harness = InmemRebacRepositoryHarness::new();

    kamu_auth_rebac_repo_tests::test_delete_property_from_entity(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_insert_duplicate_entities_relation() {
    let harness = InmemRebacRepositoryHarness::new();

    kamu_auth_rebac_repo_tests::test_try_insert_duplicate_entities_relation(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_entities_relation() {
    let harness = InmemRebacRepositoryHarness::new();

    kamu_auth_rebac_repo_tests::test_delete_entities_relation(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_relations_crossover_test() {
    let harness = InmemRebacRepositoryHarness::new();

    kamu_auth_rebac_repo_tests::test_get_relations_crossover_test(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InmemRebacRepositoryHarness {
    catalog: Catalog,
}

impl InmemRebacRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add::<InMemoryRebacRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}
