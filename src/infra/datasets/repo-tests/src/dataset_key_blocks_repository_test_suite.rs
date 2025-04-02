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
use odf::metadata::testing::MetadataFactory;
use odf::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use odf::serde::MetadataBlockSerializer;

use crate::helpers::{init_dataset_entry, init_test_account, remove_dataset_entry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_info_block(sequence_number: u64) -> DatasetKeyBlock {
    let event = MetadataFactory::set_info()
        .description("Test dataset")
        .keyword("demo")
        .build();

    let block = MetadataFactory::metadata_block(event).build();

    make_block(sequence_number, &block, MetadataEventType::SetInfo)
}

fn make_license_block(sequence_number: u64) -> DatasetKeyBlock {
    let event = MetadataFactory::set_license()
        .short_name("MIT")
        .name("MIT License")
        .build();

    let block = MetadataFactory::metadata_block(event).build();

    make_block(sequence_number, &block, MetadataEventType::SetLicense)
}

fn make_block(
    sequence_number: u64,
    block: &odf::MetadataBlock,
    kind: MetadataEventType,
) -> DatasetKeyBlock {
    let block_hash =
        odf::Multihash::from_digest_sha3_256(format!("block-{sequence_number}").as_bytes());

    let block_data = FlatbuffersMetadataBlockSerializer
        .write_manifest(block)
        .unwrap();

    DatasetKeyBlock {
        event_kind: kind,
        sequence_number,
        block_hash,
        block_payload: bytes::Bytes::from(block_data.collapse_vec()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_has_blocks(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-1");
    let dataset_name = odf::DatasetName::new_unchecked("test-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();
    assert!(!repo
        .has_blocks(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap());

    let block = make_info_block(0);
    repo.save_block(&dataset_id, &odf::BlockRef::Head, &block)
        .await
        .unwrap();
    assert!(repo
        .has_blocks(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_block_and_duplicate(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-dup");
    let dataset_name = odf::DatasetName::new_unchecked("dup-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();

    let block = make_info_block(42);
    repo.save_block(&dataset_id, &odf::BlockRef::Head, &block)
        .await
        .unwrap();

    let err = repo
        .save_block(&dataset_id, &odf::BlockRef::Head, &block)
        .await
        .unwrap_err();
    assert_matches!(err, DatasetKeyBlockSaveError::DuplicateSequenceNumber(42));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_save_blocks_batch(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-batch");
    let dataset_name = odf::DatasetName::new_unchecked("batch-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();
    let blocks = vec![
        make_info_block(1),
        make_license_block(2),
        make_info_block(3),
    ];
    repo.save_blocks_batch(&dataset_id, &odf::BlockRef::Head, &blocks)
        .await
        .unwrap();

    let all = repo
        .find_blocks_of_kind_in_range(
            &dataset_id,
            &odf::BlockRef::Head,
            MetadataEventType::SetInfo,
            None,
            100,
        )
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_latest_block_of_kind(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-latest");
    let dataset_name = odf::DatasetName::new_unchecked("latest-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();

    for i in 0..5 {
        let block = make_info_block(i);
        repo.save_block(&dataset_id, &odf::BlockRef::Head, &block)
            .await
            .unwrap();
    }

    let latest = repo
        .find_latest_block_of_kind(
            &dataset_id,
            &odf::BlockRef::Head,
            MetadataEventType::SetInfo,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.sequence_number, 4);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_blocks_of_kind_in_range(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-range");
    let dataset_name = odf::DatasetName::new_unchecked("range-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();
    for i in 1..=5 {
        repo.save_block(&dataset_id, &odf::BlockRef::Head, &make_info_block(i))
            .await
            .unwrap();
    }

    let filtered = repo
        .find_blocks_of_kind_in_range(
            &dataset_id,
            &odf::BlockRef::Head,
            MetadataEventType::SetInfo,
            Some(2),
            4,
        )
        .await
        .unwrap();

    assert_eq!(filtered.len(), 3);
    assert_eq!(filtered[0].sequence_number, 2);
    assert_eq!(filtered[2].sequence_number, 4);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_find_max_sequence_number(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-max");
    let dataset_name = odf::DatasetName::new_unchecked("max-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();
    assert_eq!(
        repo.find_max_sequence_number(&dataset_id, &odf::BlockRef::Head)
            .await
            .unwrap(),
        None
    );

    repo.save_block(&dataset_id, &odf::BlockRef::Head, &make_info_block(3))
        .await
        .unwrap();
    repo.save_block(&dataset_id, &odf::BlockRef::Head, &make_info_block(7))
        .await
        .unwrap();

    let max = repo
        .find_max_sequence_number(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap();
    assert_eq!(max, Some(7));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_blocks(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-del");
    let dataset_name = odf::DatasetName::new_unchecked("del-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();
    for i in 0..5 {
        repo.save_block(&dataset_id, &odf::BlockRef::Head, &make_info_block(i))
            .await
            .unwrap();
    }

    repo.delete_blocks_after(&dataset_id, &odf::BlockRef::Head, 2)
        .await
        .unwrap();

    let remaining = repo
        .find_blocks_of_kind_in_range(
            &dataset_id,
            &odf::BlockRef::Head,
            MetadataEventType::SetInfo,
            None,
            10,
        )
        .await
        .unwrap();
    assert_eq!(remaining.len(), 3);
    assert!(remaining.iter().all(|b| b.sequence_number <= 2));

    repo.delete_all_for_ref(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap();
    assert!(!repo
        .has_blocks(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_remove_dataset_entry_removes_key_blocks(catalog: &Catalog) {
    let test_account_id = init_test_account(catalog).await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-remove");
    let dataset_name = odf::DatasetName::new_unchecked("remove-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();

    // Add some blocks
    for i in 0..3 {
        repo.save_block(&dataset_id, &odf::BlockRef::Head, &make_info_block(i))
            .await
            .unwrap();
    }

    // Verify blocks exist
    assert!(repo
        .has_blocks(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap());

    // Remove dataset entry
    remove_dataset_entry(catalog, &dataset_id).await;

    // Verify blocks are removed
    assert!(!repo
        .has_blocks(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
