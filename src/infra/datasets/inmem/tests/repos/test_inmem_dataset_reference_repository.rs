// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use kamu_datasets_repo_tests::dataset_reference_repo;

use super::InMemoryDatasetRepositoriesHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_reference_repo::test_set_initial_reference,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_reference_repo::test_update_reference_without_cas_violation,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_reference_repo::test_update_reference_provoke_cas_violation,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_reference_repo::test_update_reference_from_stale_hash_provoke_cas_violation,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_reference_repo::test_update_missing_reference_provoke_cas_violation,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_reference_repo::test_set_and_remove_reference,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_reference_repo::test_multiple_datasets,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_reference_repo::test_reacts_to_dataset_removals,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
