// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::path::Path;
use std::sync::Arc;

use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use opendatafabric::*;
use url::Url;

use crate::utils::DummySmartTransferProtocolClient;

// Create repo/bar dataset in a repo and check it appears in searches
async fn do_test_search(tmp_workspace_dir: &Path, repo_url: Url) {
    let dataset_local_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let repo_name = RepoName::new_unchecked("repo");
    let dataset_remote_alias = DatasetAliasRemote::try_from("repo/bar").unwrap();

    let dataset_action_authorizer = Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new());

    let dataset_repo = Arc::new(
        DatasetRepositoryLocalFs::create(
            tmp_workspace_dir.join("datasets"),
            Arc::new(CurrentAccountSubject::new_test()),
            dataset_action_authorizer.clone(),
            false,
        )
        .unwrap(),
    );
    let remote_repo_reg =
        Arc::new(RemoteRepositoryRegistryImpl::create(tmp_workspace_dir.join("repos")).unwrap());
    let sync_svc = SyncServiceImpl::new(
        remote_repo_reg.clone(),
        dataset_repo.clone(),
        dataset_action_authorizer.clone(),
        Arc::new(DatasetFactoryImpl::new(IpfsGateway::default())),
        Arc::new(DummySmartTransferProtocolClient::new()),
        Arc::new(kamu::utils::ipfs_wrapper::IpfsClient::default()),
    );

    let search_svc = SearchServiceImpl::new(remote_repo_reg.clone());

    // Add repository
    remote_repo_reg
        .add_repository(&repo_name, repo_url)
        .unwrap();

    // Add and sync dataset
    dataset_repo
        .create_dataset_from_snapshot(
            None,
            MetadataFactory::dataset_snapshot()
                .name(&dataset_local_alias.dataset_name)
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    sync_svc
        .sync(
            &dataset_local_alias.as_any_ref(),
            &dataset_remote_alias.as_any_ref(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();

    // Search!
    assert_matches!(
        search_svc.search(None, SearchOptions::default()).await,
        Ok(SearchResult { datasets }) if datasets == vec![dataset_remote_alias.clone()]
    );
    assert_matches!(
        search_svc.search(Some("bar"), SearchOptions::default()).await,
        Ok(SearchResult { datasets }) if datasets == vec![dataset_remote_alias.clone()]
    );
    assert_matches!(
        search_svc.search(Some("foo"), SearchOptions::default()).await,
        Ok(SearchResult { datasets }) if datasets.is_empty()
    );
}

#[test_log::test(tokio::test)]
async fn test_search_local_fs() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    do_test_search(tmp_workspace_dir.path(), repo_url).await;
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_search_s3() {
    let s3 = LocalS3Server::new().await;
    let tmp_workspace_dir = tempfile::tempdir().unwrap();

    do_test_search(tmp_workspace_dir.path(), s3.url).await;
}
