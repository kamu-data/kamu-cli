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

use kamu::infra::*;
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

fn local_fs_repo(tempdir: &TempDir) -> DatasetRepositoryLocalFs {
    let workspace_layout = WorkspaceLayout::create(tempdir.path()).unwrap();
    DatasetRepositoryLocalFs::new(Arc::new(workspace_layout))
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let repo = local_fs_repo(&tempdir);

    test_dataset_repository_shared::test_create_dataset(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let tempdir = tempfile::tempdir().unwrap();
    let repo = local_fs_repo(&tempdir);

    test_dataset_repository_shared::test_create_dataset_from_snapshot(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_rename_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let repo = local_fs_repo(&tempdir);

    test_dataset_repository_shared::test_rename_dataset(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////
 