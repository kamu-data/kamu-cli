// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use chrono::{SubsecRound, Utc};
use dill::Catalog;
use kamu_accounts::{Account, AccountRepository, AccountType};
use kamu_datasets::{
    DatasetEntry,
    DatasetEntryByNameNotFoundError,
    DatasetEntryNotFoundError,
    DatasetEntryRepository,
    DeleteEntryDatasetError,
    GetDatasetEntryByNameError,
    GetDatasetEntryError,
    SaveDatasetEntryError,
    UpdateDatasetEntryNameError,
};
use opendatafabric::{AccountID, AccountName, DatasetID, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entry(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let dataset_entry = new_dataset_entry(&account);
    {
        let get_res = dataset_entry_repo
            .get_dataset_entry(&dataset_entry.id)
            .await;

        assert_matches!(
            get_res,
            Err(GetDatasetEntryError::NotFound(DatasetEntryNotFoundError { dataset_id: actual_dataset_id }))
                if actual_dataset_id == dataset_entry.id
        );
    }
    {
        let count_res = dataset_entry_repo.dataset_entries_count().await;

        assert_matches!(count_res, Ok(0));
    }
    {
        let save_res = dataset_entry_repo.save_dataset_entry(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let get_res = dataset_entry_repo
            .get_dataset_entry(&dataset_entry.id)
            .await;

        assert_matches!(
            get_res,
            Ok(actual_dataset_entry)
                if actual_dataset_entry == dataset_entry
        );
    }
    {
        let count_res = dataset_entry_repo.dataset_entries_count().await;

        assert_matches!(count_res, Ok(1));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entry_by_name(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let dataset_entry = new_dataset_entry(&account);
    {
        let get_res = dataset_entry_repo
            .get_dataset_entry_by_name(&dataset_entry.owner_id, &dataset_entry.name)
            .await;

        assert_matches!(
            get_res,
            Err(GetDatasetEntryByNameError::NotFound(DatasetEntryByNameNotFoundError {
                owner_id: actual_owner_id,
                dataset_name: actual_dataset_name
            }))
                if actual_owner_id == dataset_entry.owner_id
                    && actual_dataset_name == dataset_entry.name
        );
    }
    {
        let save_res = dataset_entry_repo.save_dataset_entry(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let get_res = dataset_entry_repo
            .get_dataset_entry_by_name(&dataset_entry.owner_id, &dataset_entry.name)
            .await;

        assert_matches!(
            get_res,
            Ok(actual_dataset_entry)
                if actual_dataset_entry == dataset_entry
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entries_by_owner_id(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account_1 = new_account_with_name(&account_repo, "user1").await;
    let account_2 = new_account_with_name(&account_repo, "user2").await;

    {
        let get_res = dataset_entry_repo
            .get_dataset_entries_by_owner_id(&account_1.id)
            .await;
        let expected_dataset_entries = vec![];

        assert_matches!(
            get_res,
            Ok(actual_dataset_entries)
                if actual_dataset_entries == expected_dataset_entries
        );
    }
    {
        let get_res = dataset_entry_repo
            .get_dataset_entries_by_owner_id(&account_2.id)
            .await;
        let expected_dataset_entries = vec![];

        assert_matches!(
            get_res,
            Ok(actual_dataset_entries)
                if actual_dataset_entries == expected_dataset_entries
        );
    }

    let dataset_entry_acc_1_1 = new_dataset_entry_with(&account_1, "dataset1");
    let dataset_entry_acc_1_2 = new_dataset_entry_with(&account_1, "dataset2");
    let dataset_entry_acc_2_3 = new_dataset_entry_with(&account_2, "dataset3");
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_acc_1_1)
            .await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_acc_1_2)
            .await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_acc_2_3)
            .await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let get_res = dataset_entry_repo
            .get_dataset_entries_by_owner_id(&account_1.id)
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
            .get_dataset_entries_by_owner_id(&account_2.id)
            .await;
        let expected_dataset_entries = vec![dataset_entry_acc_2_3];

        assert_matches!(
            get_res,
            Ok(actual_dataset_entries)
                if actual_dataset_entries == expected_dataset_entries
        );
    }
    {
        let count_res = dataset_entry_repo.dataset_entries_count().await;

        assert_matches!(count_res, Ok(3));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_save_duplicate_dataset_entry(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let mut dataset_entry = new_dataset_entry(&account);
    {
        let save_res = dataset_entry_repo.save_dataset_entry(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    {
        // We change the name to ensure we get a duplicate error and not a collision.
        dataset_entry.name = DatasetName::new_unchecked("another-name");

        let save_res = dataset_entry_repo.save_dataset_entry(&dataset_entry).await;

        assert_matches!(
            save_res,
            Err(SaveDatasetEntryError::Duplicate(e))
                if e.dataset_id == dataset_entry.id
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_save_dataset_entry_with_name_collision(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let dataset_entry_1 = new_dataset_entry_with(&account, "dataset");
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_1)
            .await;

        assert_matches!(save_res, Ok(_));
    }

    let same_dataset_name = dataset_entry_1.name.as_str();
    let dataset_entry_2 = new_dataset_entry_with(&account, same_dataset_name);
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_2)
            .await;

        assert_matches!(
            save_res,
            Err(SaveDatasetEntryError::NameCollision(e))
                if e.dataset_name == dataset_entry_2.name
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_try_set_same_dataset_name_for_another_owned_dataset_entry(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let dataset_entry_1 = new_dataset_entry_with(&account, "dataset1");
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_1)
            .await;

        assert_matches!(save_res, Ok(_));
    }

    let same_dataset_name_as_before = &dataset_entry_1.name;
    {
        let update_res = dataset_entry_repo
            .update_dataset_entry_name(&dataset_entry_1.id, same_dataset_name_as_before)
            .await;

        assert_matches!(update_res, Ok(_));
    }
    {
        let count_res = dataset_entry_repo.dataset_entries_count().await;

        assert_matches!(count_res, Ok(1));
    }

    let dataset_entry_2 = new_dataset_entry_with(&account, "dataset2");
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_2)
            .await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let count_res = dataset_entry_repo.dataset_entries_count().await;

        assert_matches!(count_res, Ok(2));
    }

    let same_dataset_name_as_another_owned_dataset = &dataset_entry_1.name;
    {
        let update_res = dataset_entry_repo
            .update_dataset_entry_name(
                &dataset_entry_2.id,
                same_dataset_name_as_another_owned_dataset,
            )
            .await;

        assert_matches!(
            update_res,
            Err(UpdateDatasetEntryNameError::NameCollision(e))
                if e.dataset_name == *same_dataset_name_as_another_owned_dataset
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_update_dataset_entry_name(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let dataset_entry = new_dataset_entry(&account);
    let new_name = DatasetName::new_unchecked("new-name");
    {
        let update_res = dataset_entry_repo
            .update_dataset_entry_name(&dataset_entry.id, &new_name)
            .await;

        assert_matches!(
            update_res,
            Err(UpdateDatasetEntryNameError::NotFound(DatasetEntryNotFoundError { dataset_id: actual_dataset_id }))
                if actual_dataset_id == dataset_entry.id
        );
    }
    {
        let save_res = dataset_entry_repo.save_dataset_entry(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let count_res = dataset_entry_repo.dataset_entries_count().await;

        assert_matches!(count_res, Ok(1));
    }
    {
        let update_res = dataset_entry_repo
            .update_dataset_entry_name(&dataset_entry.id, &new_name)
            .await;

        assert_matches!(update_res, Ok(_));
    }
    {
        let count_res = dataset_entry_repo.dataset_entries_count().await;

        assert_matches!(count_res, Ok(1));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_entry(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let dataset_entry = new_dataset_entry(&account);
    {
        let delete_res = dataset_entry_repo
            .delete_dataset_entry(&dataset_entry.id)
            .await;

        assert_matches!(
            delete_res,
            Err(DeleteEntryDatasetError::NotFound(DatasetEntryNotFoundError { dataset_id: actual_dataset_id }))
                if actual_dataset_id == dataset_entry.id
        );
    }
    {
        let save_res = dataset_entry_repo.save_dataset_entry(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let count_res = dataset_entry_repo.dataset_entries_count().await;

        assert_matches!(count_res, Ok(1));
    }
    {
        let delete_res = dataset_entry_repo
            .delete_dataset_entry(&dataset_entry.id)
            .await;

        assert_matches!(delete_res, Ok(_));
    }
    {
        let count_res = dataset_entry_repo.dataset_entries_count().await;

        assert_matches!(count_res, Ok(0));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn new_account_with_name(
    account_repo: &Arc<dyn AccountRepository>,
    account_name: &str,
) -> Account {
    let (_, id) = AccountID::new_generated_ed25519();

    let account = Account {
        id,
        account_name: AccountName::new_unchecked(account_name),
        email: None,
        display_name: String::new(),
        account_type: AccountType::User,
        avatar_url: None,
        registered_at: Default::default(),
        is_admin: false,
        provider: "unit-test-provider".to_string(),
        provider_identity_key: account_name.to_string(),
    };
    let create_res = account_repo.create_account(&account).await;

    assert_matches!(create_res, Ok(_));

    account
}

async fn new_account(account_repo: &Arc<dyn AccountRepository>) -> Account {
    new_account_with_name(account_repo, "unit-test-user").await
}

fn new_dataset_entry_with(owner: &Account, dataset_name: &str) -> DatasetEntry {
    let (_, dataset_id) = DatasetID::new_generated_ed25519();
    let owner_id = owner.id.clone();
    let dataset_alias = DatasetName::new_unchecked(dataset_name);
    let created_at = Utc::now().round_subsecs(6);

    DatasetEntry::new(dataset_id, owner_id, dataset_alias, created_at)
}

fn new_dataset_entry(owner: &Account) -> DatasetEntry {
    new_dataset_entry_with(owner, "dataset")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
