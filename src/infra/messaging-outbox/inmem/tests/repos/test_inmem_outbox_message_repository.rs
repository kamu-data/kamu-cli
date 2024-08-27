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
use kamu_messaging_outbox_inmem::InMemoryOutboxMessageRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_messaging_outbox_repo_tests::test_no_outbox_messages_initially,
    harness = InMemoryOutboxMessageRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_messaging_outbox_repo_tests::test_push_messages_from_several_producers,
    harness = InMemoryOutboxMessageRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_messaging_outbox_repo_tests::test_push_many_messages_and_read_parts,
    harness = InMemoryOutboxMessageRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_messaging_outbox_repo_tests::test_try_reading_above_max,
    harness = InMemoryOutboxMessageRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryOutboxMessageRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryOutboxMessageRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryOutboxMessageRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
