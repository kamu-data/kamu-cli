// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::test_dataset_repository_shared;

use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;
use tempfile::TempDir;

use std::assert_matches::assert_matches;

fn local_fs_repo(tempdir: &TempDir) -> DatasetRepositoryLocalFs {
    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    DatasetRepositoryLocalFs::new(Arc::new(workspace_layout))
}

#[tokio::test]
async fn test_create_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let repo = local_fs_repo(&tempdir);

    test_dataset_repository_shared::test_create_dataset(&repo).await;
}

#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let tempdir = tempfile::tempdir().unwrap();
    let repo = local_fs_repo(&tempdir);

    test_dataset_repository_shared::test_create_dataset_from_snapshot(&repo).await;
}

#[tokio::test]
async fn test_rename_dataset() {
    let tempdir = tempfile::tempdir().unwrap();

    let name_foo = DatasetName::new_unchecked("foo");
    let name_bar = DatasetName::new_unchecked("bar");
    let name_baz = DatasetName::new_unchecked("baz");

    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    let repo = DatasetRepositoryLocalFs::new(Arc::new(workspace_layout));

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

    create_datasets_from_snapshots(&repo, snapshots).await;

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

#[tokio::test]
async fn test_delete_dataset() {
    let tempdir = tempfile::tempdir().unwrap();

    let name_foo = DatasetName::new_unchecked("foo");
    let name_bar = DatasetName::new_unchecked("bar");

    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    let repo = DatasetRepositoryLocalFs::new(Arc::new(workspace_layout));

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

    let handles: Vec<_> = create_datasets_from_snapshots(&repo, snapshots)
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
