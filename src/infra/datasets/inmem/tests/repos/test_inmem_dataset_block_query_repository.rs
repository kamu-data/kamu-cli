// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use kamu_datasets_repo_tests::dataset_block_query_repo;

use super::InMemoryDatasetRepositoriesHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_block_query_repo::test_list_blocks_unfiltered,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_block_query_repo::test_list_blocks_by_event_types,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_block_query_repo::test_list_blocks_by_hash_prefix,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_block_query_repo::test_list_blocks_paginated,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = dataset_block_query_repo::test_list_blocks_of_unknown_dataset,
    harness = InMemoryDatasetRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
