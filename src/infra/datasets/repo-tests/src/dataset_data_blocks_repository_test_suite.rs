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
    make_add_data_block,
    make_execute_transform_block,
    remove_dataset_entry,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_has_data_blocks(catalog: &Catalog) {
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

    let repo = catalog.get_one::<dyn DatasetDataBlockRepository>().unwrap();
    assert!(
        !repo
            .has_data_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );

    // Add some AddData blocks (Root datasets can only have AddData)
    let blocks = vec![make_add_data_block(1), make_add_data_block(2)];
    repo.save_data_blocks_batch(&dataset_id, &odf::BlockRef::Head, &blocks)
        .await
        .unwrap();

    assert!(
        repo.has_data_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_data_blocks_batch(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    // Test with Root dataset (AddData blocks only)
    let root_dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-batch-root");
    let root_dataset_name = odf::DatasetName::new_unchecked("batch-root-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &root_dataset_id,
        &root_dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetDataBlockRepository>().unwrap();
    let root_blocks = vec![make_add_data_block(1), make_add_data_block(2)];
    repo.save_data_blocks_batch(&root_dataset_id, &odf::BlockRef::Head, &root_blocks)
        .await
        .unwrap();

    let root_all = repo
        .get_all_data_blocks(&root_dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap();

    assert_eq!(root_all.len(), 2);
    assert_eq!(root_all[0].sequence_number, 1);
    assert_eq!(root_all[0].event_kind, MetadataEventType::AddData);
    assert_eq!(root_all[1].sequence_number, 2);
    assert_eq!(root_all[1].event_kind, MetadataEventType::AddData);

    // Test with Derivative dataset (ExecuteTransform blocks only)
    let derivative_dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-batch-derivative");
    let derivative_dataset_name = odf::DatasetName::new_unchecked("batch-derivative-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &derivative_dataset_id,
        &derivative_dataset_name,
        odf::DatasetKind::Derivative,
    )
    .await;

    let derivative_blocks = vec![
        make_execute_transform_block(1),
        make_execute_transform_block(2),
    ];
    repo.save_data_blocks_batch(
        &derivative_dataset_id,
        &odf::BlockRef::Head,
        &derivative_blocks,
    )
    .await
    .unwrap();

    let derivative_all = repo
        .get_all_data_blocks(&derivative_dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap();

    assert_eq!(derivative_all.len(), 2);
    assert_eq!(derivative_all[0].sequence_number, 1);
    assert_eq!(
        derivative_all[0].event_kind,
        MetadataEventType::ExecuteTransform
    );
    assert_eq!(derivative_all[1].sequence_number, 2);
    assert_eq!(
        derivative_all[1].event_kind,
        MetadataEventType::ExecuteTransform
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_data_blocks_batch_duplicate_sequence_number(catalog: &Catalog) {
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

    let repo = catalog.get_one::<dyn DatasetDataBlockRepository>().unwrap();

    // Save an AddData block at sequence number 1 (Root dataset)
    let add_data_block = make_add_data_block(1);
    repo.save_data_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[add_data_block])
        .await
        .unwrap();

    // Attempt to save another AddData block at sequence number 1
    let invalid_block = make_add_data_block(1);
    let result = repo
        .save_data_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[invalid_block])
        .await;

    // Verify the error is DuplicateSequenceNumber
    assert_matches!(
        result,
        Err(DatasetDataBlockSaveError::DuplicateSequenceNumber(v)) if v == vec![1]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_data_block_by_hash(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-get-by-hash");
    let dataset_name = odf::DatasetName::new_unchecked("get-by-hash-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetDataBlockRepository>().unwrap();

    // Save an AddData block (Root dataset)
    let block = make_add_data_block(1);
    let block_hash = block.block_hash.clone();
    repo.save_data_blocks_batch(
        &dataset_id,
        &odf::BlockRef::Head,
        std::slice::from_ref(&block),
    )
    .await
    .unwrap();

    // Test contains_data_block
    assert!(
        repo.contains_data_block(&dataset_id, &block_hash)
            .await
            .unwrap()
    );

    // Test get_data_block
    let retrieved_block = repo.get_data_block(&dataset_id, &block_hash).await.unwrap();
    assert!(retrieved_block.is_some());
    let retrieved_block = retrieved_block.unwrap();
    assert_eq!(retrieved_block.sequence_number, block.sequence_number);
    assert_eq!(retrieved_block.event_kind, block.event_kind);
    assert_eq!(retrieved_block.block_hash, block.block_hash);

    // Test get_data_block_size
    let block_size = repo
        .get_data_block_size(&dataset_id, &block_hash)
        .await
        .unwrap();
    assert!(block_size.is_some());
    assert_eq!(block_size.unwrap(), block.block_payload.len());

    // Test with non-existent hash
    let non_existent_hash = odf::Multihash::from_digest_sha3_256(b"non-existent");
    assert!(
        !repo
            .contains_data_block(&dataset_id, &non_existent_hash)
            .await
            .unwrap()
    );
    let result = repo
        .get_data_block(&dataset_id, &non_existent_hash)
        .await
        .unwrap();
    assert!(result.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_page_of_data_blocks(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-pagination");
    let dataset_name = odf::DatasetName::new_unchecked("pagination-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetDataBlockRepository>().unwrap();

    // Save multiple AddData blocks (Root dataset)
    let blocks = vec![
        make_add_data_block(1),
        make_add_data_block(2),
        make_add_data_block(3),
        make_add_data_block(4),
        make_add_data_block(5),
    ];
    repo.save_data_blocks_batch(&dataset_id, &odf::BlockRef::Head, &blocks)
        .await
        .unwrap();

    // Test pagination - get first 2 blocks starting from the highest sequence
    // number
    let page = repo
        .get_page_of_data_blocks(&dataset_id, &odf::BlockRef::Head, 2, 5)
        .await
        .unwrap();

    assert_eq!(page.len(), 2);
    // Should return blocks in ascending order of sequence numbers (4, 5)
    assert_eq!(page[0].sequence_number, 4);
    assert_eq!(page[1].sequence_number, 5);
    assert_eq!(page[0].event_kind, MetadataEventType::AddData);
    assert_eq!(page[1].event_kind, MetadataEventType::AddData);

    // Test pagination - get next 2 blocks
    let page = repo
        .get_page_of_data_blocks(&dataset_id, &odf::BlockRef::Head, 2, 3)
        .await
        .unwrap();

    assert_eq!(page.len(), 2);
    assert_eq!(page[0].sequence_number, 2);
    assert_eq!(page[1].sequence_number, 3);
    assert_eq!(page[0].event_kind, MetadataEventType::AddData);
    assert_eq!(page[1].event_kind, MetadataEventType::AddData);

    // Test pagination - get remaining block
    let page = repo
        .get_page_of_data_blocks(&dataset_id, &odf::BlockRef::Head, 10, 1)
        .await
        .unwrap();

    assert_eq!(page.len(), 1);
    assert_eq!(page[0].sequence_number, 1);
    assert_eq!(page[0].event_kind, MetadataEventType::AddData);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_data_blocks(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-delete");
    let dataset_name = odf::DatasetName::new_unchecked("delete-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetDataBlockRepository>().unwrap();

    // Save some AddData blocks (Root dataset)
    let blocks = vec![make_add_data_block(1), make_add_data_block(2)];
    repo.save_data_blocks_batch(&dataset_id, &odf::BlockRef::Head, &blocks)
        .await
        .unwrap();

    // Verify blocks exist
    assert!(
        repo.has_data_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );

    // Delete all blocks
    repo.delete_all_data_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap();

    // Verify blocks are gone
    assert!(
        !repo
            .has_data_blocks_for_ref(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_remove_dataset_entry_removes_data_blocks(catalog: &Catalog) {
    let (test_account_id, test_account_name) = init_test_account(catalog).await;

    // Test with Root dataset (AddData blocks)
    let root_dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-cascade-delete-root");
    let root_dataset_name = odf::DatasetName::new_unchecked("cascade-delete-root-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &root_dataset_id,
        &root_dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetDataBlockRepository>().unwrap();

    // Save some AddData blocks
    let root_blocks = vec![make_add_data_block(1), make_add_data_block(2)];
    repo.save_data_blocks_batch(&root_dataset_id, &odf::BlockRef::Head, &root_blocks)
        .await
        .unwrap();

    // Verify blocks exist
    assert!(
        repo.has_data_blocks_for_ref(&root_dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );

    // Remove dataset entry (should cascade delete data blocks)
    remove_dataset_entry(catalog, &root_dataset_id).await;

    // Verify blocks are gone
    assert!(
        !repo
            .has_data_blocks_for_ref(&root_dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );

    // Test with Derivative dataset (ExecuteTransform blocks)
    let derivative_dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-cascade-delete-derivative");
    let derivative_dataset_name = odf::DatasetName::new_unchecked("cascade-delete-derivative-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &test_account_name,
        &derivative_dataset_id,
        &derivative_dataset_name,
        odf::DatasetKind::Derivative,
    )
    .await;

    // Save some ExecuteTransform blocks
    let derivative_blocks = vec![
        make_execute_transform_block(1),
        make_execute_transform_block(2),
    ];
    repo.save_data_blocks_batch(
        &derivative_dataset_id,
        &odf::BlockRef::Head,
        &derivative_blocks,
    )
    .await
    .unwrap();

    // Verify blocks exist
    assert!(
        repo.has_data_blocks_for_ref(&derivative_dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );

    // Remove dataset entry (should cascade delete data blocks)
    remove_dataset_entry(catalog, &derivative_dataset_id).await;

    // Verify blocks are gone
    assert!(
        !repo
            .has_data_blocks_for_ref(&derivative_dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
