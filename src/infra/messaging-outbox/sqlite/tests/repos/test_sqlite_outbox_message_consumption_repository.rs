// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::SqliteTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_messaging_outbox_sqlite::SqliteOutboxMessageConsumptionRepository;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_messaging_outbox_repo_tests::test_no_outbox_consumptions_initially,
    harness = SqliteOutboxMessageConsumptionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_messaging_outbox_repo_tests::test_create_consumption,
    harness = SqliteOutboxMessageConsumptionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_messaging_outbox_repo_tests::test_update_existing_consumption,
    harness = SqliteOutboxMessageConsumptionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_messaging_outbox_repo_tests::test_cannot_update_consumption_before_creation,
    harness = SqliteOutboxMessageConsumptionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_messaging_outbox_repo_tests::test_multiple_boundaries,
    harness = SqliteOutboxMessageConsumptionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteOutboxMessageConsumptionRepositoryHarness {
    catalog: Catalog,
}

impl SqliteOutboxMessageConsumptionRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteOutboxMessageConsumptionRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
