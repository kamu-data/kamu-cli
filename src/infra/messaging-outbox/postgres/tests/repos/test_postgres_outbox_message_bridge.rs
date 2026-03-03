// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PostgresTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_messaging_outbox_postgres::PostgresOutboxMessageBridge;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_no_outbox_messages_initially,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_push_messages_from_several_producers,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_push_many_messages_and_read_parts,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_messaging_outbox_repo_tests::test_reading_messages_above_max_with_multiple_producers,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_try_reading_above_max,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_messaging_outbox_repo_tests::test_outbox_messages_version,
    harness = PostgresOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresOutboxMessageBridgeHarness {
    catalog: Catalog,
}

impl PostgresOutboxMessageBridgeHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresOutboxMessageBridge>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
