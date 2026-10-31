// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use kamu_datasets_repo_tests::dataset_dependency_repo;

use super::InMemoryDatasetRepositoriesHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_crud_single_dependency,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_several_unrelated_dependencies,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_dependency_chain,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_dependency_fanins,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_dependency_fanouts,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_add_duplicate_dependency,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_add_dependency_twice_in_one_request,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_remove_dependency,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_remove_missing_dependency,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_remove_all_dataset_dependencies,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_dependency_repo::test_remove_orphan_dependencies,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
