// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use opendatafabric::*;

#[tokio::test]
async fn test_summary_updates() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let layout = DatasetLayout::create(tmp_dir.path()).unwrap();
    let ds = DatasetFactoryImpl::get_local_fs(layout);

    assert_matches!(
        ds.get_summary(GetSummaryOpts::default()).await,
        Err(GetSummaryError::EmptyDataset)
    );

    let block_1 = MetadataFactory::metadata_block(
        MetadataFactory::seed(DatasetKind::Root)
            .id_from(b"foo")
            .build(),
    )
    .build();

    let hash_1 = ds
        .as_metadata_chain()
        .append(block_1.clone(), AppendOpts::default())
        .await
        .unwrap();

    assert_eq!(
        ds.get_summary(GetSummaryOpts::default()).await.unwrap(),
        DatasetSummary {
            id: DatasetID::from_pub_key_ed25519(b"foo"),
            kind: DatasetKind::Root,
            last_block_hash: hash_1.clone(),
            dependencies: Vec::new(),
            last_pulled: None,
            num_records: 0,
            data_size: 0,
            checkpoints_size: 0,
        }
    );

    let block_2 = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .interval(0, 9)
            .data_size(16)
            .checkpoint_size(10)
            .build(),
    )
    .prev(&hash_1, block_1.sequence_number)
    .build();

    let hash_2 = ds
        .as_metadata_chain()
        .append(block_2.clone(), AppendOpts::default())
        .await
        .unwrap();

    // Get stale
    assert_eq!(
        ds.get_summary(GetSummaryOpts {
            update_if_stale: false,
            ..Default::default()
        })
        .await
        .unwrap(),
        DatasetSummary {
            id: DatasetID::from_pub_key_ed25519(b"foo"),
            kind: DatasetKind::Root,
            last_block_hash: hash_1.clone(),
            dependencies: Vec::new(),
            last_pulled: None,
            num_records: 0,
            data_size: 0,
            checkpoints_size: 0,
        }
    );

    assert_eq!(
        ds.get_summary(GetSummaryOpts::default()).await.unwrap(),
        DatasetSummary {
            id: DatasetID::from_pub_key_ed25519(b"foo"),
            kind: DatasetKind::Root,
            last_block_hash: hash_2.clone(),
            dependencies: Vec::new(),
            last_pulled: Some(block_2.system_time.clone()),
            num_records: 10,
            data_size: 16,
            checkpoints_size: 10,
        }
    );
}
