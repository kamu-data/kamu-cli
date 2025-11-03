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
use kamu_datasets::*;

use crate::helpers::{
    init_dataset_entry,
    init_test_account,
    make_info_block,
    make_license_block,
    make_seed_block,
    remove_dataset_entry,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_has_blocks(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-1");
    let dataset_name = odf::DatasetName::new_unchecked("test-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();
    assert!(
        !repo
            .has_key_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );

    let block = make_seed_block(odf::DatasetKind::Root);
    repo.save_key_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[block])
        .await
        .unwrap();
    assert!(
        repo.has_key_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_blocks_batch(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-batch");
    let dataset_name = odf::DatasetName::new_unchecked("batch-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();
    let blocks = vec![
        make_seed_block(odf::DatasetKind::Root),
        make_info_block(1),
        make_license_block(2),
    ];
    repo.save_key_blocks_batch(&dataset_id, &odf::BlockRef::Head, &blocks)
        .await
        .unwrap();

    let all = repo
        .get_all_key_blocks(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap();

    assert_eq!(all.len(), 3);
    assert_eq!(all[0].sequence_number, 0);
    assert_eq!(all[0].event_kind, MetadataEventType::Seed);
    assert_eq!(all[1].sequence_number, 1);
    assert_eq!(all[1].event_kind, MetadataEventType::SetInfo);
    assert_eq!(all[2].sequence_number, 2);
    assert_eq!(all[2].event_kind, MetadataEventType::SetLicense);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_blocks_batch_duplicate_sequence_number(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-duplicate");
    let dataset_name = odf::DatasetName::new_unchecked("duplicate-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();

    // Save a Seed block at sequence number 0
    let seed_block = make_seed_block(odf::DatasetKind::Root);
    repo.save_key_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[seed_block])
        .await
        .unwrap();

    // Attempt to save another block at sequence number 0
    let invalid_block = make_info_block(0);
    let result = repo
        .save_key_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[invalid_block])
        .await;

    // Verify the error is DuplicateSequenceNumber
    assert_matches!(
        result,
        Err(DatasetKeyBlockSaveError::DuplicateSequenceNumber(v)) if v == vec![0]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_match_datasets_having_blocks(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"dataset_1");
    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"dataset_2");
    let dataset_id_3 = odf::DatasetID::new_seeded_ed25519(b"dataset_3");

    let dataset_name_1 = odf::DatasetName::new_unchecked("dataset_1");
    let dataset_name_2 = odf::DatasetName::new_unchecked("dataset_2");
    let dataset_name_3 = odf::DatasetName::new_unchecked("dataset_3");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id_1,
        &dataset_name_1,
        odf::DatasetKind::Root,
    )
    .await;

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id_2,
        &dataset_name_2,
        odf::DatasetKind::Root,
    )
    .await;

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id_3,
        &dataset_name_3,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();
    let blocks = vec![
        make_seed_block(odf::DatasetKind::Root),
        make_info_block(1),
        make_license_block(2),
    ];
    repo.save_key_blocks_batch(&dataset_id_1, &odf::BlockRef::Head, &blocks)
        .await
        .unwrap();

    let blocks = vec![
        make_seed_block(odf::DatasetKind::Root),
        make_info_block(1),
        make_info_block(2),
    ];
    repo.save_key_blocks_batch(&dataset_id_2, &odf::BlockRef::Head, &blocks)
        .await
        .unwrap();

    let blocks = vec![
        make_seed_block(odf::DatasetKind::Root),
        make_info_block(1),
        make_license_block(2),
        make_license_block(3),
    ];
    repo.save_key_blocks_batch(&dataset_id_3, &odf::BlockRef::Head, &blocks)
        .await
        .unwrap();

    let dataset_ids = vec![
        dataset_id_1.clone(),
        dataset_id_2.clone(),
        dataset_id_3.clone(),
    ];

    let matches = repo
        .match_datasets_having_key_blocks(
            &dataset_ids,
            &odf::BlockRef::Head,
            kamu_datasets::MetadataEventType::SetLicense,
        )
        .await
        .unwrap();

    assert_eq!(matches.len(), 2);
    assert!(
        matches
            .iter()
            .any(|(id, key_block)| id == &dataset_id_1 && key_block.sequence_number == 2)
    );
    assert!(
        matches
            .iter()
            .any(|(id, key_block)| id == &dataset_id_3 && key_block.sequence_number == 3)
    );

    let matches = repo
        .match_datasets_having_key_blocks(
            &dataset_ids,
            &odf::BlockRef::Head,
            kamu_datasets::MetadataEventType::SetInfo,
        )
        .await
        .unwrap();

    assert_eq!(matches.len(), 3);
    assert!(
        matches
            .iter()
            .any(|(id, key_block)| id == &dataset_id_1 && key_block.sequence_number == 1)
    );
    assert!(
        matches
            .iter()
            .any(|(id, key_block)| id == &dataset_id_2 && key_block.sequence_number == 2)
    );
    assert!(
        matches
            .iter()
            .any(|(id, key_block)| id == &dataset_id_3 && key_block.sequence_number == 1)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_blocks(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-del");
    let dataset_name = odf::DatasetName::new_unchecked("del-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();
    repo.save_key_blocks_batch(
        &dataset_id,
        &odf::BlockRef::Head,
        &[make_seed_block(odf::DatasetKind::Root)],
    )
    .await
    .unwrap();
    for i in 1..5 {
        repo.save_key_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[make_info_block(i)])
            .await
            .unwrap();
    }

    repo.delete_all_key_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap();
    assert!(
        !repo
            .has_key_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_remove_dataset_entry_removes_key_blocks(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-remove");
    let dataset_name = odf::DatasetName::new_unchecked("remove-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();

    // Add some blocks
    repo.save_key_blocks_batch(
        &dataset_id,
        &odf::BlockRef::Head,
        &[make_seed_block(odf::DatasetKind::Root)],
    )
    .await
    .unwrap();
    for i in 1..4 {
        repo.save_key_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[make_info_block(i)])
            .await
            .unwrap();
    }

    // Verify blocks exist
    assert!(
        repo.has_key_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );

    // Remove dataset entry
    remove_dataset_entry(catalog, &dataset_id).await;

    // Verify blocks are removed
    assert!(
        !repo
            .has_key_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_unindexed_dataset_branches(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    // Create multiple dataset entries
    let dataset_id_1 = odf::DatasetID::new_seeded_ed25519(b"ds-unindexed-1");
    let dataset_id_2 = odf::DatasetID::new_seeded_ed25519(b"ds-unindexed-2");
    let dataset_id_3 = odf::DatasetID::new_seeded_ed25519(b"ds-unindexed-3");

    let dataset_name_1 = odf::DatasetName::new_unchecked("unindexed-ds-1");
    let dataset_name_2 = odf::DatasetName::new_unchecked("unindexed-ds-2");
    let dataset_name_3 = odf::DatasetName::new_unchecked("unindexed-ds-3");

    // Initialize all dataset entries
    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id_1,
        &dataset_name_1,
        odf::DatasetKind::Root,
    )
    .await;

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id_2,
        &dataset_name_2,
        odf::DatasetKind::Root,
    )
    .await;

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id_3,
        &dataset_name_3,
        odf::DatasetKind::Derivative,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();

    // Initially, all datasets should appear as unindexed since no blocks are saved
    let unindexed = repo.list_unindexed_dataset_branches().await.unwrap();
    assert_eq!(unindexed.len(), 3);

    // Verify all datasets are in the unindexed list
    let unindexed_ids: std::collections::HashSet<_> =
        unindexed.iter().map(|(id, _)| id.clone()).collect();
    assert!(unindexed_ids.contains(&dataset_id_1));
    assert!(unindexed_ids.contains(&dataset_id_2));
    assert!(unindexed_ids.contains(&dataset_id_3));

    // All should be for Head branch
    assert!(
        unindexed
            .iter()
            .all(|(_, block_ref)| *block_ref == odf::BlockRef::Head)
    );

    // Index dataset_1 by saving some key blocks
    let blocks_1 = vec![make_seed_block(odf::DatasetKind::Root), make_info_block(1)];
    repo.save_key_blocks_batch(&dataset_id_1, &odf::BlockRef::Head, &blocks_1)
        .await
        .unwrap();

    // Index dataset_3 by saving some key blocks
    let blocks_3 = vec![
        make_seed_block(odf::DatasetKind::Derivative),
        make_license_block(1),
    ];
    repo.save_key_blocks_batch(&dataset_id_3, &odf::BlockRef::Head, &blocks_3)
        .await
        .unwrap();

    // Now only dataset_2 should be unindexed
    let unindexed = repo.list_unindexed_dataset_branches().await.unwrap();
    assert_eq!(unindexed.len(), 1);
    assert_eq!(unindexed[0].0, dataset_id_2);
    assert_eq!(unindexed[0].1, odf::BlockRef::Head);

    // Index the remaining dataset
    let blocks_2 = vec![make_seed_block(odf::DatasetKind::Root)];
    repo.save_key_blocks_batch(&dataset_id_2, &odf::BlockRef::Head, &blocks_2)
        .await
        .unwrap();

    // Now no datasets should be unindexed
    let unindexed = repo.list_unindexed_dataset_branches().await.unwrap();
    assert_eq!(unindexed.len(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
