// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use database_common::PaginationOpts;
use dill::Catalog;
use kamu_accounts::{Account, AccountRepository};
use kamu_datasets::{
    DatasetEntriesResolution,
    DatasetEntryByNameNotFoundError,
    DatasetEntryNotFoundError,
    DatasetEntryRepository,
    DeleteEntryDatasetError,
    GetDatasetEntryByNameError,
    GetDatasetEntryError,
    SaveDatasetEntryError,
    UpdateDatasetEntryNameError,
};

use crate::helpers::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entry(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let dataset_entry = new_dataset_entry(&account, odf::DatasetKind::Root);
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

pub async fn test_stream_many_entries(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    use futures::TryStreamExt;

    {
        let get_res = dataset_entry_repo
            .get_dataset_entries(PaginationOpts {
                limit: 100,
                offset: 0,
            })
            .await
            .try_collect::<Vec<_>>()
            .await;
        let expected_dataset_entries = vec![];

        assert_matches!(
            get_res,
            Ok(actual_dataset_entries)
                if actual_dataset_entries == expected_dataset_entries
        );
    }

    let account_1 = new_account_with_name(&account_repo, "user1").await;
    let account_2 = new_account_with_name(&account_repo, "user2").await;

    let dataset_entry_acc_1_1 =
        new_dataset_entry_with(&account_1, "dataset1", odf::DatasetKind::Root);
    let dataset_entry_acc_1_2 =
        new_dataset_entry_with(&account_1, "dataset2", odf::DatasetKind::Root);
    let dataset_entry_acc_2_3 =
        new_dataset_entry_with(&account_2, "dataset3", odf::DatasetKind::Root);

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
            .get_dataset_entries(PaginationOpts {
                limit: 100,
                offset: 0,
            })
            .await
            .try_collect::<Vec<_>>()
            .await;
        assert!(get_res.is_ok());

        let mut actual_dataset_entries = get_res.unwrap();
        actual_dataset_entries.sort_by(|e1, e2| e1.name.cmp(&e2.name));

        let mut expected_dataset_entries = vec![
            dataset_entry_acc_1_1,
            dataset_entry_acc_1_2,
            dataset_entry_acc_2_3,
        ];
        expected_dataset_entries.sort_by(|e1, e2| e1.name.cmp(&e2.name));

        assert_eq!(actual_dataset_entries, expected_dataset_entries);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_multiple_entries(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    {
        let get_multiple_res = dataset_entry_repo
            .get_multiple_dataset_entries(&[])
            .await
            .unwrap();

        assert_eq!(
            get_multiple_res,
            DatasetEntriesResolution {
                resolved_entries: vec![],
                unresolved_entries: vec![]
            }
        );
    }

    let account = new_account(&account_repo).await;

    let dataset_entry_acc_1 = new_dataset_entry_with(&account, "dataset1", odf::DatasetKind::Root);
    let dataset_entry_acc_2 = new_dataset_entry_with(&account, "dataset2", odf::DatasetKind::Root);
    let dataset_entry_acc_3 = new_dataset_entry_with(&account, "dataset3", odf::DatasetKind::Root);

    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_acc_1)
            .await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_acc_2)
            .await;

        assert_matches!(save_res, Ok(_));
    }
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_acc_3)
            .await;

        assert_matches!(save_res, Ok(_));
    }

    {
        let mut get_multiple_res = dataset_entry_repo
            .get_multiple_dataset_entries(&[&dataset_entry_acc_1.id, &dataset_entry_acc_3.id])
            .await
            .unwrap();

        get_multiple_res
            .resolved_entries
            .sort_by(|e1, e2| e1.id.cmp(&e2.id));

        let mut expected_resolved_entries =
            vec![dataset_entry_acc_1.clone(), dataset_entry_acc_3.clone()];
        expected_resolved_entries.sort_by(|e1, e2| e1.id.cmp(&e2.id));

        assert_eq!(
            get_multiple_res,
            DatasetEntriesResolution {
                resolved_entries: expected_resolved_entries,
                unresolved_entries: vec![]
            }
        );
    }

    {
        let wrong_id = odf::DatasetID::new_seeded_ed25519(b"wrong_id");
        let get_multiple_res = dataset_entry_repo
            .get_multiple_dataset_entries(&[&dataset_entry_acc_2.id, &wrong_id])
            .await
            .unwrap();

        assert_eq!(
            get_multiple_res,
            DatasetEntriesResolution {
                resolved_entries: vec![dataset_entry_acc_2.clone()],
                unresolved_entries: vec![wrong_id]
            }
        );
    }

    {
        let wrong_id_1 = odf::DatasetID::new_seeded_ed25519(b"wrong_id_1");
        let wrong_id_2 = odf::DatasetID::new_seeded_ed25519(b"wrong_id_2");

        let get_multiple_res = dataset_entry_repo
            .get_multiple_dataset_entries(&[&wrong_id_1, &wrong_id_2])
            .await
            .unwrap();

        assert_eq!(
            get_multiple_res,
            DatasetEntriesResolution {
                resolved_entries: vec![],
                unresolved_entries: vec![wrong_id_1, wrong_id_2]
            }
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entry_by_name(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let dataset_entry = new_dataset_entry(&account, odf::DatasetKind::Root);
    {
        let get_res = dataset_entry_repo
            .get_dataset_entry_by_owner_and_name(&dataset_entry.owner_id, &dataset_entry.name)
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
            .get_dataset_entry_by_owner_and_name(&dataset_entry.owner_id, &dataset_entry.name)
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

    use futures::TryStreamExt;

    {
        assert_eq!(
            dataset_entry_repo
                .dataset_entries_count_by_owner_id(&account_1.id)
                .await
                .unwrap(),
            0,
        );
        assert_eq!(
            dataset_entry_repo
                .dataset_entries_count_by_owner_id(&account_2.id)
                .await
                .unwrap(),
            0,
        );

        let get_res = dataset_entry_repo
            .get_dataset_entries_by_owner_id(
                &account_1.id,
                PaginationOpts {
                    limit: 100,
                    offset: 0,
                },
            )
            .await
            .try_collect::<Vec<_>>()
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
            .get_dataset_entries_by_owner_id(
                &account_2.id,
                PaginationOpts {
                    limit: 100,
                    offset: 0,
                },
            )
            .await
            .try_collect::<Vec<_>>()
            .await;
        let expected_dataset_entries = vec![];

        assert_matches!(
            get_res,
            Ok(actual_dataset_entries)
                if actual_dataset_entries == expected_dataset_entries
        );
    }

    let dataset_entry_acc_1_1 =
        new_dataset_entry_with(&account_1, "dataset1", odf::DatasetKind::Root);
    let dataset_entry_acc_1_2 =
        new_dataset_entry_with(&account_1, "dataset2", odf::DatasetKind::Root);
    let dataset_entry_acc_2_3 =
        new_dataset_entry_with(&account_2, "dataset3", odf::DatasetKind::Root);
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
            .get_dataset_entries_by_owner_id(
                &account_1.id,
                PaginationOpts {
                    limit: 100,
                    offset: 0,
                },
            )
            .await
            .try_collect::<Vec<_>>()
            .await;
        let mut expected_dataset_entries = vec![dataset_entry_acc_1_1, dataset_entry_acc_1_2];

        expected_dataset_entries.sort_by(|e1, e2| e1.id.cmp(&e2.id));

        match get_res {
            Ok(mut actual_dataset_entries) => {
                actual_dataset_entries.sort_by(|e1, e2| e1.id.cmp(&e2.id));

                assert_eq!(expected_dataset_entries, actual_dataset_entries);
            }
            Err(e) => {
                panic!("A successful result was expected, but an error was received: {e}");
            }
        }

        assert_eq!(
            dataset_entry_repo
                .dataset_entries_count_by_owner_id(&account_1.id)
                .await
                .unwrap(),
            2,
        );
    }
    {
        let get_res = dataset_entry_repo
            .get_dataset_entries_by_owner_id(
                &account_2.id,
                PaginationOpts {
                    limit: 100,
                    offset: 0,
                },
            )
            .await
            .try_collect::<Vec<_>>()
            .await;
        let expected_dataset_entries = vec![dataset_entry_acc_2_3];

        assert_matches!(
            get_res,
            Ok(actual_dataset_entries)
                if actual_dataset_entries == expected_dataset_entries
        );

        assert_eq!(
            dataset_entry_repo
                .dataset_entries_count_by_owner_id(&account_2.id)
                .await
                .unwrap(),
            1,
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

    let mut dataset_entry = new_dataset_entry(&account, odf::DatasetKind::Root);
    {
        let save_res = dataset_entry_repo.save_dataset_entry(&dataset_entry).await;

        assert_matches!(save_res, Ok(_));
    }
    {
        // We change the name to ensure we get a duplicate error and not a collision.
        dataset_entry.name = odf::DatasetName::new_unchecked("another-name");

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

    let dataset_entry_1 = new_dataset_entry_with(&account, "dataset", odf::DatasetKind::Root);
    {
        let save_res = dataset_entry_repo
            .save_dataset_entry(&dataset_entry_1)
            .await;

        assert_matches!(save_res, Ok(_));
    }

    let same_dataset_name = dataset_entry_1.name.as_str();
    let dataset_entry_2 =
        new_dataset_entry_with(&account, same_dataset_name, odf::DatasetKind::Root);
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

    let dataset_entry_1 = new_dataset_entry_with(&account, "dataset1", odf::DatasetKind::Root);
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

    let dataset_entry_2 = new_dataset_entry_with(&account, "dataset2", odf::DatasetKind::Root);
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

    let dataset_entry = new_dataset_entry(&account, odf::DatasetKind::Root);
    let new_name = odf::DatasetName::new_unchecked("new-name");
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

pub async fn test_owner_of_entries_renamed(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account_1 = new_account_with_name(&account_repo, "user1").await;
    let account_2 = new_account_with_name(&account_repo, "user2").await;

    let entry_1 = new_dataset_entry_with(&account_1, "dataset1", odf::DatasetKind::Root);
    let entry_2 = new_dataset_entry_with(&account_1, "dataset2", odf::DatasetKind::Root);
    let entry_3 = new_dataset_entry_with(&account_2, "dataset3", odf::DatasetKind::Root);

    {
        let save_res = dataset_entry_repo.save_dataset_entry(&entry_1).await;
        assert_matches!(save_res, Ok(_));
    }
    {
        let save_res = dataset_entry_repo.save_dataset_entry(&entry_2).await;
        assert_matches!(save_res, Ok(_));
    }
    {
        let save_res = dataset_entry_repo.save_dataset_entry(&entry_3).await;
        assert_matches!(save_res, Ok(_));
    }

    let new_account_name = odf::AccountName::new_unchecked("user1-renamed");
    account_repo
        .update_account(&Account {
            account_name: new_account_name.clone(),
            ..account_1.clone()
        })
        .await
        .unwrap();

    dataset_entry_repo
        .update_owner_entries_after_rename(&account_1.id, &new_account_name)
        .await
        .unwrap();

    use futures::TryStreamExt;
    let get_res = dataset_entry_repo
        .get_dataset_entries(PaginationOpts {
            limit: 100,
            offset: 0,
        })
        .await
        .try_collect::<Vec<_>>()
        .await;
    assert!(get_res.is_ok());

    let mut actual_dataset_entries = get_res.unwrap();
    actual_dataset_entries.sort_by(|e1, e2| e1.name.cmp(&e2.name));
    assert_eq!(actual_dataset_entries[0].owner_name, "user1-renamed");
    assert_eq!(actual_dataset_entries[0].name, "dataset1");

    assert_eq!(actual_dataset_entries[1].owner_name, "user1-renamed");
    assert_eq!(actual_dataset_entries[1].name, "dataset2");

    assert_eq!(actual_dataset_entries[2].owner_name, "user2");
    assert_eq!(actual_dataset_entries[2].name, "dataset3");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_entry(catalog: &Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    let dataset_entry = new_dataset_entry(&account, odf::DatasetKind::Root);
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
