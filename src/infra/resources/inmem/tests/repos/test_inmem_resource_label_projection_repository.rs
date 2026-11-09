// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::InMemoryTransactionManager;
use database_common_macros::{database_concurrent_transactions_test, database_transactional_test};
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_resources_inmem::register_dependencies;
use kamu_resources_repo_tests::resource_label_projection_repository_test_suite as label_projection_repo_suite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_no_entries_initially,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_replace_entries_then_find,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_replace_entries_overwrites_previous_set,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_replace_entries_with_empty_slice_clears,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_entries_isolated_by_resource_id,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture = label_projection_repo_suite::test_rolled_back_replace_keeps_previous_entries,
    harness = InMemoryResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryResourceLabelProjectionRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryResourceLabelProjectionRepositoryHarness {
    pub fn new() -> Self {
        Self {
            catalog: Self::catalog_builder().build(),
        }
    }

    pub fn new_transactional() -> Self {
        let mut catalog_builder = Self::catalog_builder();
        catalog_builder.add::<InMemoryTransactionManager>();
        Self {
            catalog: catalog_builder.build(),
        }
    }

    fn catalog_builder() -> CatalogBuilder {
        let mut catalog_builder = CatalogBuilder::new();
        register_dependencies(&mut catalog_builder);
        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
