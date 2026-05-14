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
use kamu_configuration_inmem::InMemorySecretSetProjectionRepository;
use kamu_configuration_repo_tests::secret_set_projection_repository_test_suite as secret_set_repo;
use kamu_resources_inmem::InMemoryResourceRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_entries_empty_initially,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_replace_and_get_entries,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_find_entry_by_key,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_entries_isolated_by_resource,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_entries_isolated_by_generation,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_get_latest_entries_before_generation,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_replace_entries_concurrent_modification,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_cleanup_entries_before_generation,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_cleanup_does_not_affect_other_resources,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_replace_preserves_stable_identity_and_creation_time,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_get_latest_entries,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = secret_set_repo::test_delete_all_entries,
    harness = InMemorySecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemorySecretSetProjectionRepositoryHarness {
    catalog: Catalog,
}

impl InMemorySecretSetProjectionRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryResourceRepository>();
        catalog_builder.add::<InMemorySecretSetProjectionRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
