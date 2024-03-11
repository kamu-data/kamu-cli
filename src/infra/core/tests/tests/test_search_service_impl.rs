// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use dill::Component;
use event_bus::EventBus;
use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use opendatafabric::*;
use url::Url;

// Create repo/bar dataset in a repo and check it appears in searches
async fn do_test_search(tmp_workspace_dir: &Path, repo_url: Url) {
    let dataset_local_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let repo_name = RepoName::new_unchecked("repo");
    let dataset_remote_alias = DatasetAliasRemote::try_from("repo/bar").unwrap();

    let catalog = dill::CatalogBuilder::new()
        .add::<EventBus>()
        .add::<DependencyGraphServiceInMemory>()
        .add_value(CurrentAccountSubject::new_test())
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(tmp_workspace_dir.join("datasets"))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .add_value(RemoteRepositoryRegistryImpl::create(tmp_workspace_dir.join("repos")).unwrap())
        .bind::<dyn RemoteRepositoryRegistry, RemoteRepositoryRegistryImpl>()
        .add_value(IpfsGateway::default())
        .add_value(kamu::utils::ipfs_wrapper::IpfsClient::default())
        .add::<auth::DummyOdfServerAccessTokenResolver>()
        .add::<DatasetFactoryImpl>()
        .add::<SyncServiceImpl>()
        .add::<DummySmartTransferProtocolClient>()
        .add::<SearchServiceImpl>()
        .build();

    let remote_repo_reg = catalog.get_one::<dyn RemoteRepositoryRegistry>().unwrap();
    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let sync_svc = catalog.get_one::<dyn SyncService>().unwrap();
    let search_svc = catalog.get_one::<dyn SearchService>().unwrap();

    // Add repository
    remote_repo_reg
        .add_repository(&repo_name, repo_url)
        .unwrap();

    // Add and sync dataset
    dataset_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name(dataset_local_alias.clone())
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
    assert_eq!(
        search_svc
            .search(None, SearchOptions::default())
            .await
            .unwrap(),
        SearchResult {
            datasets: vec![SearchResultDataset {
                id: None,
                alias: dataset_remote_alias.clone(),
                kind: None,
                num_blocks: None,
                num_records: None,
                estimated_size: None,
            }]
        }
    );

    assert_eq!(
        search_svc
            .search(Some("bar"), SearchOptions::default())
            .await
            .unwrap(),
        SearchResult {
            datasets: vec![SearchResultDataset {
                id: None,
                alias: dataset_remote_alias.clone(),
                kind: None,
                num_blocks: None,
                num_records: None,
                estimated_size: None,
            }]
        }
    );

    assert_eq!(
        search_svc
            .search(Some("foo"), SearchOptions::default())
            .await
            .unwrap(),
        SearchResult { datasets: vec![] }
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
