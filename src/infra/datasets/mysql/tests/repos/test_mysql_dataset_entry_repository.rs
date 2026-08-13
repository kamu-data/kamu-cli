// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use kamu_datasets_repo_tests::dataset_entry_repo;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_get_dataset_entry,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_stream_many_entries,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_get_multiple_entries,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_get_dataset_entry_by_name,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_get_dataset_entries_by_owner_id,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_get_dataset_entries_by_owner_and_name,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_try_save_duplicate_dataset_entry,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_try_save_dataset_entry_with_name_collision,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_try_set_same_dataset_name_for_another_owned_dataset_entry,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_update_dataset_entry_name,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_owner_of_entries_renamed,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = mysql,
    fixture = dataset_entry_repo::test_delete_dataset_entry,
    harness = MySqlDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MySqlDatasetEntryRepositoryHarness {
    catalog: dill::Catalog,
}

impl MySqlDatasetEntryRepositoryHarness {
    pub fn new(mysql_pool: sqlx::MySqlPool) -> Self {
        let mut catalog_builder = dill::CatalogBuilder::new();

        catalog_builder.add_value(mysql_pool);
        catalog_builder.add::<database_common::MySqlTransactionManager>();

        catalog_builder.add::<kamu_accounts_mysql::MySqlAccountRepository>();
        catalog_builder.add::<kamu_datasets_mysql::MySqlDatasetEntryRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
