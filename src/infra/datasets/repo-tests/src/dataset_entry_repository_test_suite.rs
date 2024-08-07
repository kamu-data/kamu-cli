// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use dill::Catalog;
use kamu_datasets::{
    DatasetEntry,
    DatasetEntryRepository,
    DatasetNotFoundError,
    DeleteDatasetError,
    GetDatasetError,
    SaveDatasetError,
    UpdateDatasetAliasError,
};
use opendatafabric::{AccountID, AccountName, DatasetAlias, DatasetID, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entry(catalog: &Catalog) {
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let dataset_entry = new_dataset_entry();
    {
        let get_res = dataset_entry_repo.get_dataset(&dataset_entry.id).await;

        assert_matches!(
            get_res,
            Err(GetDatasetError::NotFound(DatasetNotFoundError::ByDatasetId(actual_dataset_id)))
                if actual_dataset_id == dataset_entry.id
        );
    }
    {
        let save_res = dataset_entry_repo.save_dataset(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let get_res = dataset_entry_repo.get_dataset(&dataset_entry.id).await;

        assert_matches!(
            get_res,
            Ok(actual_dataset_entry)
                if actual_dataset_entry == dataset_entry
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entries_by_owner_id(catalog: &Catalog) {
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let (_, owner_id_1) = AccountID::new_generated_ed25519();
    let (_, owner_id_2) = AccountID::new_generated_ed25519();

    let dataset_entry_acc_1_1 = new_dataset_entry_with(owner_id_1.clone(), "kamu", "dataset1");
    let dataset_entry_acc_1_2 = new_dataset_entry_with(owner_id_1.clone(), "kamu", "dataset2");
    let dataset_entry_acc_2_3 = new_dataset_entry_with(owner_id_2.clone(), "user", "dataset3");
    {
        let save_res = dataset_entry_repo
            .save_dataset(&dataset_entry_acc_1_1)
            .await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let save_res = dataset_entry_repo
            .save_dataset(&dataset_entry_acc_1_2)
            .await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let save_res = dataset_entry_repo
            .save_dataset(&dataset_entry_acc_2_3)
            .await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let get_res = dataset_entry_repo
            .get_datasets_by_owner_id(&owner_id_1)
            .await;
        let mut expected_dataset_entries = vec![dataset_entry_acc_1_1, dataset_entry_acc_1_2];

        expected_dataset_entries.sort();

        match get_res {
            Ok(mut actual_dataset_entries) => {
                actual_dataset_entries.sort();

                assert_eq!(expected_dataset_entries, actual_dataset_entries);
            }
            Err(e) => {
                panic!("A successful result was expected, but an error was received: {e}");
            }
        }
    }
    {
        let get_res = dataset_entry_repo
            .get_datasets_by_owner_id(&owner_id_2)
            .await;
        let expected_dataset_entries = vec![dataset_entry_acc_2_3];

        assert_matches!(
            get_res,
            Ok(actual_dataset_entries)
                if actual_dataset_entries == expected_dataset_entries
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_save_duplicate_dataset_entry(catalog: &Catalog) {
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let dataset_entry = new_dataset_entry();
    {
        let save_res = dataset_entry_repo.save_dataset(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let save_res = dataset_entry_repo.save_dataset(&dataset_entry).await;

        assert_matches!(
            save_res,
            Err(SaveDatasetError::Duplicate(e))
                if e.dataset_id == dataset_entry.id
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_set_same_dataset_alias(catalog: &Catalog) {
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let dataset_entry = new_dataset_entry();
    {
        let save_res = dataset_entry_repo.save_dataset(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    let same_alias = dataset_entry.alias;
    {
        let update_res = dataset_entry_repo
            .update_dataset_alias(&dataset_entry.id, &same_alias)
            .await;

        assert_matches!(
            update_res,
            Err(UpdateDatasetAliasError::SameAlias(e))
                if e.dataset_id == dataset_entry.id
                    && e.dataset_alias == same_alias
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_same_dataset_alias(catalog: &Catalog) {
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let dataset_entry = new_dataset_entry();
    let new_alias = dataset_alias("kamu", "new-alias");
    {
        let update_res = dataset_entry_repo
            .update_dataset_alias(&dataset_entry.id, &new_alias)
            .await;

        assert_matches!(
            update_res,
            Err(UpdateDatasetAliasError::NotFound(DatasetNotFoundError::ByDatasetId(actual_dataset_id)))
                if actual_dataset_id == dataset_entry.id
        );
    }
    {
        let save_res = dataset_entry_repo.save_dataset(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let update_res = dataset_entry_repo
            .update_dataset_alias(&dataset_entry.id, &new_alias)
            .await;

        assert_matches!(update_res, Ok(_));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_entry(catalog: &Catalog) {
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let dataset_entry = new_dataset_entry();
    {
        let delete_res = dataset_entry_repo.delete_dataset(&dataset_entry.id).await;

        assert_matches!(
            delete_res,
            Err(DeleteDatasetError::NotFound(DatasetNotFoundError::ByDatasetId(actual_dataset_id)))
                if actual_dataset_id == dataset_entry.id
        );
    }
    {
        let save_res = dataset_entry_repo.save_dataset(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn dataset_alias(account_name: &str, dataset_name: &str) -> DatasetAlias {
    let account_name = AccountName::new_unchecked(account_name);
    let dataset_name = DatasetName::new_unchecked(dataset_name);

    DatasetAlias::new(Some(account_name), dataset_name)
}

fn new_dataset_entry_with(
    owner_id: AccountID,
    account_name: &str,
    dataset_name: &str,
) -> DatasetEntry {
    let (_, dataset_id) = DatasetID::new_generated_ed25519();
    let alias = dataset_alias(account_name, dataset_name);

    DatasetEntry::new(dataset_id, owner_id, alias)
}

fn new_dataset_entry() -> DatasetEntry {
    let (_, owner_id) = AccountID::new_generated_ed25519();

    new_dataset_entry_with(owner_id, "kamu", "dataset")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
