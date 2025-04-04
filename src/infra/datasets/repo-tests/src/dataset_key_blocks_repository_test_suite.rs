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

fn make_seed_block() -> DatasetKeyBlock {
    let event = MetadataFactory::seed(odf::DatasetKind::Root).build();

    let block = MetadataFactory::metadata_block(event).build();

    make_block(0, &block, MetadataEventType::Seed)
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

    let block = make_seed_block();
    repo.save_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[block])
        .await
        .unwrap();
    assert!(repo
        .has_blocks(&dataset_id, &odf::BlockRef::Head)
        .await
        .unwrap());
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
    let blocks = vec![make_seed_block(), make_info_block(1), make_license_block(2)];
    repo.save_blocks_batch(&dataset_id, &odf::BlockRef::Head, &blocks)
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
    let test_account_id = init_test_account(catalog).await;
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-duplicate");
    let dataset_name = odf::DatasetName::new_unchecked("duplicate-ds");

    init_dataset_entry(
        catalog,
        &test_account_id,
        &dataset_id,
        &dataset_name,
        odf::DatasetKind::Root,
    )
    .await;

    let repo = catalog.get_one::<dyn DatasetKeyBlockRepository>().unwrap();

    // Save a Seed block at sequence number 0
    let seed_block = make_seed_block();
    repo.save_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[seed_block])
        .await
        .unwrap();

    // Attempt to save another block at sequence number 0
    let invalid_block = make_info_block(0);
    let result = repo
        .save_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[invalid_block])
        .await;

    // Verify the error is DuplicateSequenceNumber
    assert_matches!(
        result,
        Err(DatasetKeyBlockSaveError::DuplicateSequenceNumber(v)) if v == vec![0]
    );
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
    repo.save_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[make_seed_block()])
        .await
        .unwrap();
    for i in 1..5 {
        repo.save_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[make_info_block(i)])
            .await
            .unwrap();
    }

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
    repo.save_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[make_seed_block()])
        .await
        .unwrap();
    for i in 1..4 {
        repo.save_blocks_batch(&dataset_id, &odf::BlockRef::Head, &[make_info_block(i)])
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
