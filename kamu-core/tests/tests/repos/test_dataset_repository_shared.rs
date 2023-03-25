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

    let create_result = repo.create_dataset_from_snapshot(snapshot).await.unwrap();

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

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rename_dataset(repo: &dyn DatasetRepository) {
    let name_foo = DatasetName::new_unchecked("foo");
    let name_bar = DatasetName::new_unchecked("bar");
    let name_baz = DatasetName::new_unchecked("baz");

    let snapshots = vec![
        MetadataFactory::dataset_snapshot()
            .name("foo")
            .kind(DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build(),
        MetadataFactory::dataset_snapshot()
            .name("bar")
            .kind(DatasetKind::Derivative)
            .push_event(MetadataFactory::set_transform(["foo"]).build())
            .build(),
    ];

    repo.create_datasets_from_snapshots(snapshots).await;

    assert_matches!(
        repo.rename_dataset(&name_baz.as_local_ref(), &name_foo)
            .await,
        Err(RenameDatasetError::NotFound(_))
    );

    assert_matches!(
        repo.rename_dataset(&name_foo.as_local_ref(), &name_bar)
            .await,
        Err(RenameDatasetError::NameCollision(_))
    );

    repo.rename_dataset(&name_foo.as_local_ref(), &name_baz)
        .await
        .unwrap();

    let baz = repo.get_dataset(&name_baz.as_local_ref()).await.unwrap();

    use futures::StreamExt;
    assert_eq!(baz.as_metadata_chain().iter_blocks().count().await, 2);
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset(repo: &dyn DatasetRepository) {
    let name_foo = DatasetName::new_unchecked("foo");
    let name_bar = DatasetName::new_unchecked("bar");

    let snapshots = vec![
        MetadataFactory::dataset_snapshot()
            .name("foo")
            .kind(DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build(),
        MetadataFactory::dataset_snapshot()
            .name("bar")
            .kind(DatasetKind::Derivative)
            .push_event(MetadataFactory::set_transform(["foo"]).build())
            .build(),
    ];

    let handles: Vec<_> = repo
        .create_datasets_from_snapshots(snapshots)
        .await
        .into_iter()
        .map(|(_, r)| r.unwrap().dataset_handle)
        .collect();

    assert_matches!(
        repo.delete_dataset(&name_foo.as_local_ref()).await,
        Err(DeleteDatasetError::DanglingReference(e)) if e.children == vec![handles[1].clone()]
    );

    assert!(repo.get_dataset(&name_foo.as_local_ref()).await.is_ok());
    assert!(repo.get_dataset(&name_bar.as_local_ref()).await.is_ok());

    repo.delete_dataset(&name_bar.as_local_ref()).await.unwrap();
    repo.delete_dataset(&name_foo.as_local_ref()).await.unwrap();

    assert_matches!(
        repo.get_dataset(&name_foo.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_),
    )
}

/////////////////////////////////////////////////////////////////////////////////////////
