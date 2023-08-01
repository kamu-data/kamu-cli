// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::testing::LocalS3Server;
use kamu::utils::s3_context::S3Context;
use kamu::DatasetRepositoryS3;
use kamu_core::CurrentAccountSubject;

use super::test_dataset_repository_shared;

/////////////////////////////////////////////////////////////////////////////////////////

async fn s3_repo(s3: &LocalS3Server) -> DatasetRepositoryS3 {
    let s3_context = S3Context::from_url(&s3.url).await;
    DatasetRepositoryS3::new(
        s3_context,
        Arc::new(CurrentAccountSubject::new_test()),
        false,
    )
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(&s3).await;

    test_dataset_repository_shared::test_create_dataset(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(&s3).await;

    test_dataset_repository_shared::test_create_dataset_from_snapshot(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(&s3).await;

    test_dataset_repository_shared::test_rename_dataset(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(&s3).await;

    test_dataset_repository_shared::test_delete_dataset(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(&s3).await;

    test_dataset_repository_shared::test_iterate_datasets(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////
