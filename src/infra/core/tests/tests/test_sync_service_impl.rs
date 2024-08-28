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
use std::str::FromStr;

use dill::Component;
use kamu::domain::*;
use kamu::testing::*;
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use opendatafabric::*;
use time_source::SystemTimeSourceDefault;
use url::Url;

use crate::utils::IpfsDaemon;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FILE_DATA_ARRAY_SIZE: usize = 32;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_in_sync(
    dataset_repo: &DatasetRepositoryLocalFs,
    lhs: impl Into<DatasetRef>,
    rhs: impl Into<DatasetRef>,
) {
    let lhs_layout = dataset_repo.get_dataset_layout(&lhs.into()).await.unwrap();
    let rhs_layout = dataset_repo.get_dataset_layout(&rhs.into()).await.unwrap();
    DatasetTestHelper::assert_datasets_in_sync(&lhs_layout, &rhs_layout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct AuthorizationExpectations {
    pub d1_reads: usize,
    pub d1_writes: usize,
    pub d2_reads: usize,
    pub d2_writes: usize,
}

impl Default for AuthorizationExpectations {
    fn default() -> Self {
        Self {
            d1_reads: 8,
            d2_reads: 2,
            d1_writes: 1,
            d2_writes: 4,
        }
    }
}

fn construct_authorizer(
    authorization_expectations: &AuthorizationExpectations,
    d1_alias: &DatasetAlias,
    d2_alias: &DatasetAlias,
) -> impl auth::DatasetActionAuthorizer {
    MockDatasetActionAuthorizer::new()
        .expect_check_read_dataset(d1_alias, authorization_expectations.d1_reads, true)
        .expect_check_read_dataset(d2_alias, authorization_expectations.d2_reads, true)
        .expect_check_write_dataset(d1_alias, authorization_expectations.d1_writes, true)
        .expect_check_write_dataset(d2_alias, authorization_expectations.d2_writes, true)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn do_test_sync(
    tmp_workspace_dir: &Path,
    push_ref: &DatasetRefRemote,
    pull_ref: &DatasetRefRemote,
    ipfs: Option<(IpfsGateway, IpfsClient)>,
    auth_expectations: AuthorizationExpectations,
) {
    // Tests sync between "foo" -> remote -> "bar"
    let dataset_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let dataset_alias_2 = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));
    let is_ipfs = ipfs.is_none();

    let (ipfs_gateway, ipfs_client) = ipfs.unwrap_or_default();

    let dataset_authorizer =
        construct_authorizer(&auth_expectations, &dataset_alias, &dataset_alias_2);

    let datasets_dir = tmp_workspace_dir.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let catalog = dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add_value(ipfs_gateway)
        .add_value(ipfs_client)
        .add_value(CurrentAccountSubject::new_test())
        .add_value(dataset_authorizer)
        .bind::<dyn auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(datasets_dir)
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
        .add_value(RemoteReposDir::new(tmp_workspace_dir.join("repos")))
        .add::<RemoteRepositoryRegistryImpl>()
        .add::<auth::DummyOdfServerAccessTokenResolver>()
        .add::<DatasetFactoryImpl>()
        .add::<SyncServiceImpl>()
        .add::<DummySmartTransferProtocolClient>()
        .build();

    let sync_svc = catalog.get_one::<dyn SyncService>().unwrap();
    let dataset_repo = catalog.get_one::<DatasetRepositoryLocalFs>().unwrap();

    // Dataset does not exist locally / remotely
    assert_matches!(
        sync_svc
            .sync(
                &dataset_alias.as_any_ref(),
                &push_ref.as_any_ref(),
                SyncOptions::default(),
                None,
            )
            .await,
        Err(SyncError::DatasetNotFound(e)) if e.dataset_ref == dataset_alias.as_any_ref()
    );

    assert_matches!(
        sync_svc
            .sync(
                &pull_ref.as_any_ref(),
                &dataset_alias_2.as_any_ref(),
                SyncOptions::default(),
                None,
            )
            .await,
        Err(SyncError::DatasetNotFound(e)) if e.dataset_ref == pull_ref.as_any_ref()
    );

    // Add dataset
    let snapshot = MetadataFactory::dataset_snapshot()
        .name(dataset_alias.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_data_schema().build())
        .build();

    let b1 = dataset_repo
        .create_dataset_from_snapshot(snapshot)
        .await
        .unwrap()
        .create_dataset_result
        .head;

    // Initial sync ///////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc.sync(
            &dataset_alias.as_any_ref(),
            &push_ref.as_any_ref(),
            SyncOptions { create_if_not_exists: false, ..Default::default() },
            None
        ).await,
        Err(SyncError::DatasetNotFound(e)) if e.dataset_ref == push_ref.as_any_ref()
    );

    assert_matches!(
        sync_svc.sync(&dataset_alias.as_any_ref(), &push_ref.as_any_ref(),  SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 2,
            ..
        }) if new_head == b1
    );

    assert_matches!(
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_alias_2.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 2,
            ..
        }) if new_head == b1
    );

    assert_in_sync(&dataset_repo, &dataset_alias, &dataset_alias_2).await;

    // Subsequent sync ////////////////////////////////////////////////////////
    let _b2 = DatasetTestHelper::append_random_data(
        dataset_repo.as_ref(),
        &dataset_alias,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let b3 = DatasetTestHelper::append_random_data(
        dataset_repo.as_ref(),
        &dataset_alias,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    assert_matches!(
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_alias.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DestinationAhead(DestinationAheadError {src_head, dst_head, dst_ahead_size: 2 }))
        if src_head == b1 && dst_head == b3
    );

    assert_matches!(
        sync_svc.sync(&dataset_alias.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
            ..
        }) if old_head.as_ref() == Some(&b1) && new_head == b3
    );

    assert_matches!(
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_alias_2.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
            ..
        }) if old_head.as_ref() == Some(&b1) && new_head == b3
    );

    assert_in_sync(&dataset_repo, &dataset_alias, &dataset_alias_2).await;

    // Up to date /////////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc
            .sync(
                &dataset_alias.as_any_ref(),
                &push_ref.as_any_ref(),
                SyncOptions::default(),
                None
            )
            .await,
        Ok(SyncResult::UpToDate)
    );

    assert_matches!(
        sync_svc
            .sync(
                &pull_ref.as_any_ref(),
                &dataset_alias_2.as_any_ref(),
                SyncOptions::default(),
                None
            )
            .await,
        Ok(SyncResult::UpToDate)
    );

    assert_in_sync(&dataset_repo, &dataset_alias, &dataset_alias_2).await;

    // Datasets out-of-sync on push //////////////////////////////////////////////

    // Push a new block into dataset_2 (which we were pulling into before)
    let exta_head = DatasetTestHelper::append_random_data(
        dataset_repo.as_ref(),
        &dataset_alias_2,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    assert_matches!(
        sync_svc.sync(&dataset_alias_2.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 1,
            ..
        }) if old_head == Some(b3.clone()) && new_head == exta_head
    );

    // Try push from dataset_1
    assert_matches!(
        sync_svc.sync(&dataset_alias.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DestinationAhead(DestinationAheadError { src_head, dst_head, dst_ahead_size: 1 }))
        if src_head == b3 && dst_head == exta_head
    );

    // Try push from dataset_1 with --force: it should abandon the diverged_head
    // block
    assert_matches!(
        sync_svc
            .sync(
                &dataset_alias.as_any_ref(),
                &push_ref.as_any_ref(),
                SyncOptions {
                    force: true,
                    ..SyncOptions::default()
                },
                None
            )
            .await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 4, // full resynchronization: seed, b1, b2, b3
            ..
        }) if old_head == Some(exta_head.clone()) && new_head == b3
    );

    // Try pulling dataset_2: should fail, destination is ahead
    assert_matches!(
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_alias_2.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DestinationAhead(DestinationAheadError { src_head, dst_head, dst_ahead_size: 1}))
        if src_head == b3 && dst_head == exta_head
    );

    // Try pulling dataset_2 with --force: should abandon diverged_head
    assert_matches!(
        sync_svc
            .sync(
                &pull_ref.as_any_ref(),
                &dataset_alias_2.as_any_ref(),
                SyncOptions {
                    force: true,
                    .. SyncOptions::default()
                },
                None
            )
            .await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 4, // full resynchronization: seed, b1, b2, b3
            ..
        }) if old_head == Some(exta_head.clone()) && new_head == b3
    );

    // Datasets complex divergence //////////////////////////////////////////////

    let _b4 = DatasetTestHelper::append_random_data(
        dataset_repo.as_ref(),
        &dataset_alias,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let b5 = DatasetTestHelper::append_random_data(
        dataset_repo.as_ref(),
        &dataset_alias,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let b4_alt = DatasetTestHelper::append_random_data(
        dataset_repo.as_ref(),
        &dataset_alias_2,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    assert_matches!(
        sync_svc.sync(&dataset_alias.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
            ..
        }) if old_head.as_ref() == Some(&b3) && new_head == b5
    );

    assert_matches!(
        sync_svc.sync(&dataset_alias_2.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DatasetsDiverged(DatasetsDivergedError {
            src_head,
            dst_head,
            detail: Some(DatasetsDivergedErrorDetail {
                uncommon_blocks_in_src,
                uncommon_blocks_in_dst
            })
        }))
        if src_head == b4_alt && dst_head == b5 && uncommon_blocks_in_src == 1 && uncommon_blocks_in_dst == 2
    );

    // Datasets corrupted transfer flow /////////////////////////////////////////
    if is_ipfs {
        let _b6 = DatasetTestHelper::append_random_data(
            dataset_repo.as_ref(),
            &dataset_alias,
            FILE_DATA_ARRAY_SIZE,
        )
        .await;

        let dir_files =
            std::fs::read_dir(tmp_workspace_dir.join("datasets/foo/checkpoints")).unwrap();
        for file_info in dir_files {
            std::fs::remove_file(file_info.unwrap().path()).unwrap();
        }

        for _i in 0..15 {
            DatasetTestHelper::append_random_data(
                dataset_repo.as_ref(),
                &dataset_alias,
                FILE_DATA_ARRAY_SIZE,
            )
            .await;
        }

        assert_matches!(
            sync_svc
            .sync(
                &dataset_alias.as_any_ref(),
                &push_ref.as_any_ref(),
                SyncOptions::default(),
                None,
            )
            .await,
            Err(SyncError::Corrupted(CorruptedSourceError {
                message,
                ..
            })) if message == *"Source checkpoint file is missing"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sync_to_from_local_fs() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    do_test_sync(
        tmp_workspace_dir.path(),
        &DatasetRefRemote::from(&repo_url),
        &DatasetRefRemote::from(&repo_url),
        None,
        AuthorizationExpectations::default(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_sync_to_from_s3() {
    let s3 = LocalS3Server::new().await;
    let tmp_workspace_dir = tempfile::tempdir().unwrap();

    do_test_sync(
        tmp_workspace_dir.path(),
        &DatasetRefRemote::from(&s3.url),
        &DatasetRefRemote::from(&s3.url),
        None,
        AuthorizationExpectations::default(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sync_from_http() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let push_repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    let server = HttpFileServer::new(tmp_repo_dir.path());
    let pull_repo_url = Url::from_str(&format!("http://{}/", server.local_addr())).unwrap();

    let _server_hdl = tokio::spawn(server.run());

    do_test_sync(
        tmp_workspace_dir.path(),
        &DatasetRefRemote::from(push_repo_url),
        &DatasetRefRemote::from(pull_repo_url),
        None,
        AuthorizationExpectations::default(),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_sync_to_from_ipfs() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();

    let ipfs_daemon = IpfsDaemon::new().await;
    let ipfs_client = ipfs_daemon.client();
    let key_id = ipfs_client.key_gen("test").await.unwrap();
    let ipns_url = Url::from_str(&format!("ipns://{key_id}/")).unwrap();

    do_test_sync(
        tmp_workspace_dir.path(),
        &DatasetRefRemote::from(&ipns_url),
        &DatasetRefRemote::from(&ipns_url),
        Some((
            IpfsGateway {
                url: Url::parse(&format!("http://127.0.0.1:{}", ipfs_daemon.http_port())).unwrap(),
                pre_resolve_dnslink: true,
            },
            ipfs_client,
        )),
        AuthorizationExpectations {
            d1_reads: 7,
            ..Default::default()
        },
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
