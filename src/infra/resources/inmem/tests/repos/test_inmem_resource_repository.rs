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
use kamu_resources_inmem::InMemoryResourceRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_no_resources_initially,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_create_and_find_resource,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_create_resource_duplicate_fails,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_update_resource,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_update_resource_wrong_event_id_fails,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_update_resource_optimistic_locking,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_list_resource_ids_with_pagination,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_list_resource_snapshots_by_kind,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_list_all_resource_snapshots,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_count_resources,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_resources_repo_tests::test_find_deleted_resource_not_returned,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryResourceRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryResourceRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryResourceRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
