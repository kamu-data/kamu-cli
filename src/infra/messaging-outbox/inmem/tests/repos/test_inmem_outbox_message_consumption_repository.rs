// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, CatalogBuilder};
use kamu_messaging_outbox_inmem::InMemoryOutboxMessageConsumptionRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_outbox_consumptions_initially() {
    let harness = InmemOutboxMessageConsumptionRepositoryHarness::new();
    kamu_messaging_outbox_repo_tests::test_no_outbox_consumptions_initially(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_consumption() {
    let harness = InmemOutboxMessageConsumptionRepositoryHarness::new();
    kamu_messaging_outbox_repo_tests::test_create_consumption(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_existing_consumption() {
    let harness = InmemOutboxMessageConsumptionRepositoryHarness::new();
    kamu_messaging_outbox_repo_tests::test_update_existing_consumption(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cannot_update_consumption_before_creation() {
    let harness = InmemOutboxMessageConsumptionRepositoryHarness::new();
    kamu_messaging_outbox_repo_tests::test_cannot_update_consumption_before_creation(
        &harness.catalog,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_multiple_boundaries() {
    let harness = InmemOutboxMessageConsumptionRepositoryHarness::new();
    kamu_messaging_outbox_repo_tests::test_multiple_boundaries(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InmemOutboxMessageConsumptionRepositoryHarness {
    catalog: Catalog,
}

impl InmemOutboxMessageConsumptionRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryOutboxMessageConsumptionRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
