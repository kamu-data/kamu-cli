// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::{auth, CurrentAccountSubject, TEST_ACCOUNT_NAME};
use kamu::testing::LocalS3Server;
use kamu::utils::s3_context::S3Context;
use kamu::DatasetRepositoryS3;
use opendatafabric::AccountName;

use super::test_dataset_repository_shared;
use crate::mock_dataset_action_authorizer;

/////////////////////////////////////////////////////////////////////////////////////////

async fn s3_repo(
    s3: &LocalS3Server,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    multi_tenant: bool,
) -> DatasetRepositoryS3 {
    let s3_context = S3Context::from_url(&s3.url).await;
    DatasetRepositoryS3::new(
        s3_context,
        Arc::new(CurrentAccountSubject::new_test()),
        dataset_action_authorizer,
        multi_tenant,
    )
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        false,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        true,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset(
        &repo,
        Some(AccountName::new_unchecked(TEST_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_same_name_multiple_tenants() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        true,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset_same_name_multiple_tenants(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_from_snapshot() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        false,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset_from_snapshot(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_create_dataset_from_snapshot_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        true,
    )
    .await;

    test_dataset_repository_shared::test_create_dataset_from_snapshot(
        &repo,
        Some(AccountName::new_unchecked(TEST_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(mock_dataset_action_authorizer::expecting_write_mock(1)),
        false,
    )
    .await;

    test_dataset_repository_shared::test_rename_dataset(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(mock_dataset_action_authorizer::expecting_write_mock(1)),
        true,
    )
    .await;

    test_dataset_repository_shared::test_rename_dataset(
        &repo,
        Some(AccountName::new_unchecked(TEST_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_dataset_same_name_multiple_tenants() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(mock_dataset_action_authorizer::expecting_write_mock(1)),
        true,
    )
    .await;

    test_dataset_repository_shared::test_rename_dataset_same_name_multiple_tenants(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_rename_unauthorized() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(mock_dataset_action_authorizer::denying_mock(
            TEST_ACCOUNT_NAME,
        )),
        true,
    )
    .await;

    test_dataset_repository_shared::test_rename_dataset_unauthroized(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        false,
    )
    .await;

    test_dataset_repository_shared::test_delete_dataset(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_dataset_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        true,
    )
    .await;

    test_dataset_repository_shared::test_delete_dataset(
        &repo,
        Some(AccountName::new_unchecked(TEST_ACCOUNT_NAME)),
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_delete_unauthorized() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(mock_dataset_action_authorizer::denying_mock(
            TEST_ACCOUNT_NAME,
        )),
        true,
    )
    .await;

    test_dataset_repository_shared::test_delete_dataset_unauthroized(&repo, None).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        false,
    )
    .await;

    test_dataset_repository_shared::test_iterate_datasets(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[tokio::test]
async fn test_iterate_datasets_multi_tenant() {
    let s3 = LocalS3Server::new().await;
    let repo = s3_repo(
        &s3,
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        true,
    )
    .await;

    test_dataset_repository_shared::test_iterate_datasets_multi_tenant(&repo).await;
}

/////////////////////////////////////////////////////////////////////////////////////////
