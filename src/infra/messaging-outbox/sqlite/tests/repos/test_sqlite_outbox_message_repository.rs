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
use kamu_messaging_outbox_sqlite::SqliteOutboxMessageRepository;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_messaging_outbox_repo_tests::test_no_outbox_messages_initially,
    harness = SqliteOutboxMessageRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_messaging_outbox_repo_tests::test_push_messages_from_several_producers,
    harness = SqliteOutboxMessageRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_messaging_outbox_repo_tests::test_push_many_messages_and_read_parts,
    harness = SqliteOutboxMessageRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_messaging_outbox_repo_tests::test_try_reading_above_max,
    harness = SqliteOutboxMessageRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteOutboxMessageRepositoryHarness {
    catalog: Catalog,
}

impl SqliteOutboxMessageRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteOutboxMessageRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
