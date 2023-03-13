// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu::{domain::*, testing::MetadataFactory};
use opendatafabric::{DatasetKind, DatasetName};

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_dataset(repo: &dyn DatasetRepository) {
    let dataset_name = DatasetName::new_unchecked("foo");

    assert_matches!(
        repo.get_dataset(&dataset_name.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );

    let builder = repo.create_dataset(&dataset_name).await.unwrap();
    let chain = builder.as_dataset().as_metadata_chain();

    chain
        .append(
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build(),
            AppendOpts::default(),
        )
        .await
        .unwrap();

    // Not finalized yet
    assert_matches!(
        repo.get_dataset(&dataset_name.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );

    let hdl = builder.finish().await.unwrap();
    assert_eq!(hdl.name, dataset_name);

    assert!(repo.get_dataset(&dataset_name.as_local_ref()).await.is_ok());
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_dataset_from_snapshot(repo: &dyn DatasetRepository) {
    let dataset_name = DatasetName::new_unchecked("foo");

    assert_matches!(
        repo.get_dataset(&dataset_name.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );

    let snapshot = MetadataFactory::dataset_snapshot()
        .name("foo")
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result = create_dataset_from_snapshot(repo, snapshot).await.unwrap();

    let dataset = repo
        .get_dataset(&create_result.dataset_handle.into())
        .await
        .unwrap();

    let actual_head = dataset
        .as_metadata_chain()
        .get_ref(&BlockRef::Head)
        .await
        .unwrap();

    assert_eq!(actual_head, create_result.head);
}
