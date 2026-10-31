// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use kamu_datasets_repo_tests::dataset_data_blocks_repo;

use super::InMemoryDatasetRepositoriesHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_data_blocks_repo::test_has_data_blocks,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_data_blocks_repo::test_save_data_blocks_batch,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_data_blocks_repo::test_save_data_blocks_large_batch,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_data_blocks_repo::test_save_data_blocks_batch_duplicate_sequence_number,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_data_blocks_repo::test_get_data_block_by_hash,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_data_blocks_repo::test_get_page_of_data_blocks,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_data_blocks_repo::test_delete_data_blocks,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_data_blocks_repo::test_remove_dataset_entry_removes_data_blocks,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
