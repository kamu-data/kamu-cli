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
use kamu_resources_inmem::{InMemoryResourceLabelProjectionRepository, InMemoryResourceRepository};
use kamu_resources_repo_tests::resource_label_projection_repository_test_suite as label_projection_repo_suite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = label_projection_repo_suite::test_no_entries_initially,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = label_projection_repo_suite::test_replace_entries_then_find,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = label_projection_repo_suite::test_replace_entries_overwrites_previous_set,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = label_projection_repo_suite::test_replace_entries_with_empty_slice_clears,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = label_projection_repo_suite::test_entries_isolated_by_resource_id,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryResourceLabelProjectionRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryResourceLabelProjectionRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryResourceLabelProjectionRepository>();
        catalog_builder.add::<InMemoryResourceRepository>();
        catalog_builder.add::<InMemoryAccountRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
