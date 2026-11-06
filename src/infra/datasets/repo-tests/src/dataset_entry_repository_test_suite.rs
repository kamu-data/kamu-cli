// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use database_common::PaginationOpts;
use database_common::testing::{
    RacingTransactionOutcome,
    in_committed_transaction,
    race_transactions,
};
use internal_error::InternalError;
use kamu_accounts::{Account, AccountRepository};
use kamu_datasets::{
    DatasetEntriesDeletionResult,
    DatasetEntriesResolution,
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
use pretty_assertions::{assert_eq, assert_matches};

use crate::helpers::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entry(catalog: &dill::Catalog) {
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

pub async fn test_stream_many_entries(catalog: &dill::Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    use futures::TryStreamExt;

    {
        let get_res = dataset_entry_repo
            .get_dataset_entries(PaginationOpts::from_max_results(100))
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
            .get_dataset_entries(PaginationOpts::from_max_results(100))
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

pub async fn test_get_multiple_entries(catalog: &dill::Catalog) {
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
            .get_multiple_dataset_entries(&[
                Cow::Borrowed(&dataset_entry_acc_1.id),
                Cow::Borrowed(&dataset_entry_acc_3.id),
            ])
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
            .get_multiple_dataset_entries(&[
                Cow::Borrowed(&dataset_entry_acc_2.id),
                Cow::Borrowed(&wrong_id),
            ])
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
            .get_multiple_dataset_entries(&[Cow::Borrowed(&wrong_id_1), Cow::Borrowed(&wrong_id_2)])
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

pub async fn test_get_dataset_entry_by_name(catalog: &dill::Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;
    let not_saved_dataset_entry = new_dataset_entry(&account, odf::DatasetKind::Root);

    // Not saved.
    assert_matches!(
        dataset_entry_repo
            .get_dataset_entry_by_owner_and_name(&not_saved_dataset_entry.owner_id, &not_saved_dataset_entry.name)
            .await,
        Err(GetDatasetEntryByNameError::NotFound(DatasetEntryByNameNotFoundError {
            owner_id: actual_owner_id,
            dataset_name: actual_dataset_name
        }))
            if actual_owner_id == not_saved_dataset_entry.owner_id
                && actual_dataset_name == not_saved_dataset_entry.name
    );

    let dataset_entry = not_saved_dataset_entry;

    assert_matches!(
        dataset_entry_repo.save_dataset_entry(&dataset_entry).await,
        Ok(_)
    );

    // Regular case.
    assert_matches!(
        dataset_entry_repo
            .get_dataset_entry_by_owner_and_name(&dataset_entry.owner_id, &dataset_entry.name)
            .await,
        Ok(actual_dataset_entry)
            if actual_dataset_entry == dataset_entry
    );
    // Mixed case.
    assert_matches!(
        dataset_entry_repo
            .get_dataset_entry_by_owner_and_name(
                &dataset_entry.owner_id,
                &odf::DatasetName::new_unchecked("dAtAsEt"),
            )
            .await,
        Ok(actual_dataset_entry)
            if actual_dataset_entry == dataset_entry
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entries_by_owner_and_name(catalog: &dill::Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    {
        let found_entries = dataset_entry_repo
            .get_dataset_entries_by_owner_and_name(&[])
            .await
            .unwrap();

        assert_eq!([] as [DatasetEntry; _], *found_entries);
    }

    let account_1 = new_account_with_name(&account_repo, "account_1").await;
    let account_2 = new_account_with_name(&account_repo, "account_2").await;

    let account_1_dataset_1 =
        new_dataset_entry_with(&account_1, "dataset1", odf::DatasetKind::Root);
    let account_1_dataset_2 =
        new_dataset_entry_with(&account_1, "dataset2", odf::DatasetKind::Root);
    let account_2_dataset_3 =
        new_dataset_entry_with(&account_2, "dataset3", odf::DatasetKind::Root);

    assert_matches!(
        dataset_entry_repo
            .save_dataset_entry(&account_1_dataset_1)
            .await,
        Ok(_)
    );
    assert_matches!(
        dataset_entry_repo
            .save_dataset_entry(&account_1_dataset_2)
            .await,
        Ok(_)
    );
    assert_matches!(
        dataset_entry_repo
            .save_dataset_entry(&account_2_dataset_3)
            .await,
        Ok(_)
    );

    {
        // Regular case.
        let found_entries = dataset_entry_repo
            .get_dataset_entries_by_owner_and_name(&[
                &(account_1.id.clone(), account_1_dataset_1.name.clone()),
                &(account_1.id.clone(), account_1_dataset_2.name.clone()),
                &(account_2.id.clone(), account_2_dataset_3.name.clone()),
            ])
            .await
            .unwrap();
        let found_entries_for_eq = found_entries
            .iter()
            .map(|e| (e.owner_name.as_str(), e.name.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(
            [
                ("account_1", "dataset1"),
                ("account_1", "dataset2"),
                ("account_2", "dataset3")
            ],
            *found_entries_for_eq
        );
        // Mixed case.
        let found_entries = dataset_entry_repo
            .get_dataset_entries_by_owner_and_name(&[
                &(
                    account_1.id.clone(),
                    odf::DatasetName::new_unchecked("dAtAsEt1"),
                ),
                &(
                    account_1.id.clone(),
                    odf::DatasetName::new_unchecked("dAtAsEt2"),
                ),
                &(
                    account_2.id.clone(),
                    odf::DatasetName::new_unchecked("dAtAsEt3"),
                ),
            ])
            .await
            .unwrap();
        let found_entries_for_eq = found_entries
            .iter()
            .map(|e| (e.owner_name.as_str(), e.name.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(
            [
                ("account_1", "dataset1"),
                ("account_1", "dataset2"),
                ("account_2", "dataset3")
            ],
            *found_entries_for_eq
        );
    }

    {
        let not_found_account_id = odf::AccountID::new_seeded_ed25519(b"not-found-account-id");

        // Regular case.
        let found_entries = dataset_entry_repo
            .get_dataset_entries_by_owner_and_name(&[
                &(
                    not_found_account_id.clone(),
                    account_1_dataset_1.name.clone(),
                ),
                &(account_1.id.clone(), account_1_dataset_2.name.clone()),
            ])
            .await
            .unwrap();
        let found_entries_for_eq = found_entries
            .iter()
            .map(|e| (e.owner_name.as_str(), e.name.as_str()))
            .collect::<Vec<_>>();

        assert_eq!([("account_1", "dataset2")], *found_entries_for_eq);

        // Mixed case.
        let found_entries = dataset_entry_repo
            .get_dataset_entries_by_owner_and_name(&[
                &(
                    not_found_account_id,
                    odf::DatasetName::new_unchecked("dAtAsEt1"),
                ),
                &(
                    account_1.id.clone(),
                    odf::DatasetName::new_unchecked("dAtAsEt2"),
                ),
            ])
            .await
            .unwrap();
        let found_entries_for_eq = found_entries
            .iter()
            .map(|e| (e.owner_name.as_str(), e.name.as_str()))
            .collect::<Vec<_>>();

        assert_eq!([("account_1", "dataset2")], *found_entries_for_eq);
    }

    {
        let not_found_dataset_name = odf::DatasetName::new_unchecked("not-found-dataset-name");

        // Regular case.
        let found_entries = dataset_entry_repo
            .get_dataset_entries_by_owner_and_name(&[
                &(account_1.id.clone(), account_1_dataset_1.name.clone()),
                &(account_1.id.clone(), not_found_dataset_name.clone()),
            ])
            .await
            .unwrap();
        let found_entries_for_eq = found_entries
            .iter()
            .map(|e| (e.owner_name.as_str(), e.name.as_str()))
            .collect::<Vec<_>>();

        assert_eq!([("account_1", "dataset1")], *found_entries_for_eq);

        // Mixed case.
        let found_entries = dataset_entry_repo
            .get_dataset_entries_by_owner_and_name(&[
                &(
                    account_1.id.clone(),
                    odf::DatasetName::new_unchecked("dAtAsEt1"),
                ),
                &(account_1.id.clone(), not_found_dataset_name),
            ])
            .await
            .unwrap();
        let found_entries_for_eq = found_entries
            .iter()
            .map(|e| (e.owner_name.as_str(), e.name.as_str()))
            .collect::<Vec<_>>();

        assert_eq!([("account_1", "dataset1")], *found_entries_for_eq);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_entries_by_owner_id(catalog: &dill::Catalog) {
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
            .get_dataset_entries_by_owner_id(&account_1.id, PaginationOpts::from_max_results(100))
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
            .get_dataset_entries_by_owner_id(&account_2.id, PaginationOpts::from_max_results(100))
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
            .get_dataset_entries_by_owner_id(&account_1.id, PaginationOpts::from_max_results(100))
            .await
            .try_collect::<Vec<_>>()
            .await;
        // Entries come ordered by dataset name
        let expected_dataset_entries = vec![dataset_entry_acc_1_1, dataset_entry_acc_1_2];

        match get_res {
            Ok(actual_dataset_entries) => {
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
            .get_dataset_entries_by_owner_id(&account_2.id, PaginationOpts::from_max_results(100))
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

pub async fn test_try_save_duplicate_dataset_entry(catalog: &dill::Catalog) {
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

pub async fn test_try_save_dataset_entry_with_name_collision(catalog: &dill::Catalog) {
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

pub async fn test_try_set_same_dataset_name_for_another_owned_dataset_entry(
    catalog: &dill::Catalog,
) {
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

pub async fn test_update_dataset_entry_name(catalog: &dill::Catalog) {
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

pub async fn test_owner_of_entries_renamed(catalog: &dill::Catalog) {
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
        .get_dataset_entries(PaginationOpts::from_max_results(100))
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

pub async fn test_delete_dataset_entry(catalog: &dill::Catalog) {
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

pub async fn test_delete_multiple_dataset_entries(catalog: &dill::Catalog) {
    let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
    let dataset_entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();

    let account = new_account(&account_repo).await;

    {
        let delete_res = dataset_entry_repo.delete_dataset_entries(&[]).await;

        assert_matches!(
            delete_res,
            Ok(DatasetEntriesDeletionResult {
                deleted_dataset_ids,
                missing_dataset_ids,
            }) if deleted_dataset_ids.is_empty() && missing_dataset_ids.is_empty()
        );
    }

    let dataset_entry_1 = new_dataset_entry_with(&account, "dataset1", odf::DatasetKind::Root);
    let dataset_entry_2 = new_dataset_entry_with(&account, "dataset2", odf::DatasetKind::Root);
    let dataset_entry_3 = new_dataset_entry_with(&account, "dataset3", odf::DatasetKind::Root);
    let missing_dataset_id = odf::DatasetID::new_seeded_ed25519(b"missing-dataset-id");

    assert_matches!(
        dataset_entry_repo
            .save_dataset_entry(&dataset_entry_1)
            .await,
        Ok(_)
    );
    assert_matches!(
        dataset_entry_repo
            .save_dataset_entry(&dataset_entry_2)
            .await,
        Ok(_)
    );
    assert_matches!(
        dataset_entry_repo
            .save_dataset_entry(&dataset_entry_3)
            .await,
        Ok(_)
    );

    {
        let delete_res = dataset_entry_repo
            .delete_dataset_entries(&[
                Cow::Borrowed(&dataset_entry_1.id),
                Cow::Borrowed(&missing_dataset_id),
                Cow::Borrowed(&dataset_entry_2.id),
                Cow::Borrowed(&dataset_entry_1.id),
                Cow::Borrowed(&missing_dataset_id),
            ])
            .await;

        assert_matches!(
            delete_res,
            Ok(DatasetEntriesDeletionResult {
                deleted_dataset_ids,
                missing_dataset_ids,
            }) if deleted_dataset_ids == vec![dataset_entry_1.id.clone(), dataset_entry_2.id.clone()]
                && missing_dataset_ids == vec![missing_dataset_id.clone()]
        );
    }

    {
        assert_eq!(dataset_entry_repo.dataset_entries_count().await.unwrap(), 1);
        assert_eq!(
            dataset_entry_repo
                .dataset_entries_count_by_owner_id(&account.id)
                .await
                .unwrap(),
            1
        );
    }
    {
        assert_matches!(
            dataset_entry_repo
                .get_dataset_entry(&dataset_entry_1.id)
                .await,
            Err(GetDatasetEntryError::NotFound(DatasetEntryNotFoundError {
                dataset_id
            })) if dataset_id == dataset_entry_1.id
        );
        assert_matches!(
            dataset_entry_repo
                .get_dataset_entry_by_owner_and_name(&account.id, &dataset_entry_2.name)
                .await,
            Err(GetDatasetEntryByNameError::NotFound(
                DatasetEntryByNameNotFoundError {
                    owner_id,
                    dataset_name,
                }
            )) if owner_id == account.id && dataset_name == dataset_entry_2.name
        );
        assert_matches!(
            dataset_entry_repo
                .get_dataset_entry(&dataset_entry_3.id)
                .await,
            Ok(actual_dataset_entry) if actual_dataset_entry == dataset_entry_3
        );
        assert_matches!(
            dataset_entry_repo
                .get_dataset_entry_by_owner_and_name(&account.id, &dataset_entry_3.name)
                .await,
            Ok(actual_dataset_entry) if actual_dataset_entry == dataset_entry_3
        );
    }
    {
        let delete_res = dataset_entry_repo
            .delete_dataset_entries(&[
                Cow::Borrowed(&dataset_entry_3.id),
                Cow::Borrowed(&dataset_entry_3.id),
            ])
            .await;

        assert_matches!(
            delete_res,
            Ok(DatasetEntriesDeletionResult {
                deleted_dataset_ids,
                missing_dataset_ids,
            }) if deleted_dataset_ids == vec![dataset_entry_3.id.clone()]
                && missing_dataset_ids.is_empty()
        );
        assert_eq!(dataset_entry_repo.dataset_entries_count().await.unwrap(), 0);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Two transactions of one owner take the same name, which differs only in
/// case: the one that commits first keeps it, the other never commits
pub async fn test_save_same_name_in_concurrent_transactions(catalog: &dill::Catalog) {
    let (owner, _) = init_committed_entries(catalog, &[]).await;

    let first_entry = new_dataset_entry_with(&owner, "foo", odf::DatasetKind::Root);
    let second_entry = new_dataset_entry_with(&owner, "Foo", odf::DatasetKind::Root);
    let first_id = first_entry.id.clone();

    let outcome = race_transactions(
        catalog,
        |catalog| async move {
            save_entry(&catalog, &first_entry).await.unwrap();
        },
        move |catalog| async move { save_entry(&catalog, &second_entry).await },
    )
    .await;

    assert_matches!(
        outcome,
        RacingTransactionOutcome::FailedAtWrite(SaveDatasetEntryError::NameCollision(_))
            | RacingTransactionOutcome::FailedAtCommit(_)
    );
    let committed_id = in_committed_transaction(catalog, |catalog| async move {
        catalog
            .get_one::<dyn DatasetEntryRepository>()
            .unwrap()
            .get_dataset_entry_by_owner_and_name(&owner.id, &odf::DatasetName::new_unchecked("foo"))
            .await
            .unwrap()
            .id
    })
    .await;
    assert_eq!(committed_id, first_id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn save_entry(
    catalog: &dill::Catalog,
    entry: &DatasetEntry,
) -> Result<(), SaveDatasetEntryError> {
    catalog
        .get_one::<dyn DatasetEntryRepository>()
        .unwrap()
        .save_dataset_entry(entry)
        .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Two transactions rename different entries of one owner to names that differ
/// only in case: the one that commits first takes the name, the other never
/// commits
pub async fn test_rename_to_same_name_in_concurrent_transactions(catalog: &dill::Catalog) {
    let (owner, entries) = init_committed_entries(
        catalog,
        &[
            ("foo", odf::DatasetKind::Root),
            ("bar", odf::DatasetKind::Root),
        ],
    )
    .await;
    let (foo_id, bar_id) = (entries[0].id.clone(), entries[1].id.clone());
    let second_bar_id = bar_id.clone();

    let outcome = race_transactions(
        catalog,
        |catalog| async move {
            rename_entry(&catalog, &foo_id, "baz").await.unwrap();
        },
        move |catalog| async move { rename_entry(&catalog, &second_bar_id, "Baz").await },
    )
    .await;

    assert_matches!(
        outcome,
        RacingTransactionOutcome::FailedAtWrite(UpdateDatasetEntryNameError::NameCollision(_))
            | RacingTransactionOutcome::FailedAtCommit(_)
    );
    let (baz_id, bar_name) = in_committed_transaction(catalog, |catalog| async move {
        let entry_repo = catalog.get_one::<dyn DatasetEntryRepository>().unwrap();
        let baz_id = entry_repo
            .get_dataset_entry_by_owner_and_name(&owner.id, &odf::DatasetName::new_unchecked("baz"))
            .await
            .unwrap()
            .id;
        let bar_name = entry_repo.get_dataset_entry(&bar_id).await.unwrap().name;
        (baz_id, bar_name)
    })
    .await;
    assert_eq!(baz_id, entries[0].id);
    assert_eq!(bar_name, odf::DatasetName::new_unchecked("bar"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn rename_entry(
    catalog: &dill::Catalog,
    dataset_id: &odf::DatasetID,
    new_name: &str,
) -> Result<(), UpdateDatasetEntryNameError> {
    catalog
        .get_one::<dyn DatasetEntryRepository>()
        .unwrap()
        .update_dataset_entry_name(dataset_id, &odf::DatasetName::new_unchecked(new_name))
        .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Two transactions rename one entry to different names: both commit, the one
/// that commits last sets the name, and the other name stays free
pub async fn test_rename_in_concurrent_transactions(catalog: &dill::Catalog) {
    let (owner, entries) =
        init_committed_entries(catalog, &[("foo", odf::DatasetKind::Root)]).await;
    let foo_id = entries[0].id.clone();
    let second_foo_id = foo_id.clone();

    let outcome = race_transactions(
        catalog,
        |catalog| async move {
            rename_entry(&catalog, &foo_id, "bar").await.unwrap();
        },
        move |catalog| async move { rename_entry(&catalog, &second_foo_id, "baz").await },
    )
    .await;

    assert_matches!(outcome, RacingTransactionOutcome::Committed);
    let other_entry = new_dataset_entry_with(&owner, "bar", odf::DatasetKind::Root);
    let (committed_name, other_entry_saved) =
        in_committed_transaction(catalog, |catalog| async move {
            let committed_name = catalog
                .get_one::<dyn DatasetEntryRepository>()
                .unwrap()
                .get_dataset_entry(&entries[0].id)
                .await
                .unwrap()
                .name;
            let other_entry_saved = save_entry(&catalog, &other_entry).await;
            (committed_name, other_entry_saved)
        })
        .await;
    assert_eq!(committed_name, odf::DatasetName::new_unchecked("baz"));
    assert_matches!(other_entry_saved, Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Two transactions rename the owner of one entry: both commit, and the one
/// that commits last sets the owner name
pub async fn test_rename_owner_in_concurrent_transactions(catalog: &dill::Catalog) {
    let (owner, entries) =
        init_committed_entries(catalog, &[("foo", odf::DatasetKind::Root)]).await;
    let owner_id = owner.id.clone();

    let outcome = race_transactions(
        catalog,
        |catalog| async move {
            rename_owner(&catalog, &owner.id, "alice").await.unwrap();
        },
        move |catalog| async move { rename_owner(&catalog, &owner_id, "bob").await },
    )
    .await;

    assert_matches!(outcome, RacingTransactionOutcome::Committed);
    let committed_entry = get_committed_entry(catalog, &entries[0].id).await;
    assert_eq!(
        committed_entry.owner_name,
        odf::AccountName::new_unchecked("bob")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One transaction renames an entry while another renames its owner: both
/// commit, and the entry keeps both changes
pub async fn test_rename_and_rename_owner_in_concurrent_transactions(catalog: &dill::Catalog) {
    let (owner, entries) =
        init_committed_entries(catalog, &[("foo", odf::DatasetKind::Root)]).await;
    let foo_id = entries[0].id.clone();

    let outcome = race_transactions(
        catalog,
        |catalog| async move {
            rename_entry(&catalog, &foo_id, "bar").await.unwrap();
        },
        move |catalog| async move { rename_owner(&catalog, &owner.id, "bob").await },
    )
    .await;

    assert_matches!(outcome, RacingTransactionOutcome::Committed);
    let committed_entry = get_committed_entry(catalog, &entries[0].id).await;
    assert_eq!(
        (committed_entry.name, committed_entry.owner_name),
        (
            odf::DatasetName::new_unchecked("bar"),
            odf::AccountName::new_unchecked("bob")
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn rename_owner(
    catalog: &dill::Catalog,
    owner_id: &odf::AccountID,
    new_owner_name: &str,
) -> Result<(), InternalError> {
    catalog
        .get_one::<dyn DatasetEntryRepository>()
        .unwrap()
        .update_owner_entries_after_rename(
            owner_id,
            &odf::AccountName::new_unchecked(new_owner_name),
        )
        .await
}

async fn get_committed_entry(catalog: &dill::Catalog, dataset_id: &odf::DatasetID) -> DatasetEntry {
    in_committed_transaction(catalog, |catalog| async move {
        catalog
            .get_one::<dyn DatasetEntryRepository>()
            .unwrap()
            .get_dataset_entry(dataset_id)
            .await
            .unwrap()
    })
    .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
