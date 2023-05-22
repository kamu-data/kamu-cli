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
use std::sync::Arc;

use kamu::domain::*;
use kamu::infra::utils::ipfs_wrapper::IpfsClient;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;
use url::Url;

use crate::utils::{DummySmartTransferProtocolClient, HttpFileServer, IpfsDaemon};

async fn append_random_data(
    local_repo: &dyn DatasetRepository,
    dataset_ref: impl Into<DatasetRef>,
) -> Multihash {
    let tmp_dir = tempfile::tempdir().unwrap();

    let ds = local_repo.get_dataset(&dataset_ref.into()).await.unwrap();

    let prev_data = ds
        .as_metadata_chain()
        .iter_blocks()
        .filter_map_ok(|(_, b)| match b.event {
            MetadataEvent::AddData(e) => Some(e),
            _ => None,
        })
        .try_first()
        .await
        .unwrap();

    let data_path = tmp_dir.path().join("data");
    let checkpoint_path = tmp_dir.path().join("checkpoint");
    ParquetWriterHelper::from_sample_data(&data_path).unwrap();
    create_random_file(&checkpoint_path).await;

    let input_checkpoint = prev_data
        .as_ref()
        .and_then(|e| e.output_checkpoint.as_ref())
        .map(|c| c.physical_hash.clone());

    let prev_offset = prev_data
        .as_ref()
        .and_then(|e| e.output_data.as_ref())
        .map(|d| d.interval.end)
        .unwrap_or(-1);
    let data_interval = OffsetInterval {
        start: prev_offset + 1,
        end: prev_offset + 10,
    };

    ds.commit_add_data(
        input_checkpoint,
        Some(data_interval),
        Some(data_path),
        Some(checkpoint_path),
        None,
        None,
        CommitOpts::default(),
    )
    .await
    .unwrap()
    .new_head
}

fn assert_in_sync(
    workspace_layout: &WorkspaceLayout,
    dataset_alias_1: &DatasetAlias,
    dataset_alias_2: &DatasetAlias,
) {
    let dataset_1_layout = workspace_layout.dataset_layout(dataset_alias_1);
    let dataset_2_layout = workspace_layout.dataset_layout(dataset_alias_2);

    DatasetTestHelper::assert_datasets_in_sync(&dataset_1_layout, &dataset_2_layout);
}

async fn create_random_file(path: &Path) -> usize {
    use rand::RngCore;

    let mut data = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut data);

    std::fs::write(path, data).unwrap();
    data.len()
}

async fn do_test_sync(
    tmp_workspace_dir: &Path,
    push_ref: &DatasetRefRemote,
    pull_ref: &DatasetRefRemote,
    ipfs: Option<(IpfsGateway, IpfsClient)>,
) {
    // Tests sync between "foo" -> remote -> "bar"
    let dataset_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let dataset_alias_2 = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let (ipfs_gateway, ipfs_client) = ipfs.unwrap_or_default();

    let workspace_layout = Arc::new(WorkspaceLayout::create(tmp_workspace_dir).unwrap());
    let local_repo = Arc::new(DatasetRepositoryLocalFs::new(workspace_layout.clone()));
    let remote_repo_reg = Arc::new(RemoteRepositoryRegistryImpl::new(workspace_layout.clone()));
    let dataset_factory = Arc::new(DatasetFactoryImpl::new(ipfs_gateway));

    let sync_svc = SyncServiceImpl::new(
        remote_repo_reg.clone(),
        local_repo.clone(),
        dataset_factory,
        Arc::new(DummySmartTransferProtocolClient::new()),
        Arc::new(ipfs_client),
    );

    // Dataset does not exist locally / remotely //////////////////////////////
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
        .name(&dataset_alias.dataset_name)
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let b1 = local_repo
        .create_dataset_from_snapshot(snapshot)
        .await
        .unwrap()
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
        }) if new_head == b1
    );

    assert_matches!(
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_alias_2.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 2,
        }) if new_head == b1
    );

    assert_in_sync(&workspace_layout, &dataset_alias, &dataset_alias_2);

    // Subsequent sync ////////////////////////////////////////////////////////
    let _b2 = append_random_data(local_repo.as_ref(), &dataset_alias).await;

    let b3 = append_random_data(local_repo.as_ref(), &dataset_alias).await;

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
        }) if old_head.as_ref() == Some(&b1) && new_head == b3
    );

    assert_matches!(
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_alias_2.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
        }) if old_head.as_ref() == Some(&b1) && new_head == b3
    );

    assert_in_sync(&workspace_layout, &dataset_alias, &dataset_alias_2);

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

    assert_in_sync(&workspace_layout, &dataset_alias, &dataset_alias_2);

    // Datasets out-of-sync on push //////////////////////////////////////////////

    // Push a new block into dataset_2 (which we were pulling into before)
    let exta_head = append_random_data(local_repo.as_ref(), &dataset_alias_2).await;

    assert_matches!(
        sync_svc.sync(&dataset_alias_2.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 1,
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
        }) if old_head == Some(exta_head.clone()) && new_head == b3
    );

    // Datasets complex divergence //////////////////////////////////////////////

    let _b4 = append_random_data(local_repo.as_ref(), &dataset_alias).await;

    let b5 = append_random_data(local_repo.as_ref(), &dataset_alias).await;

    let b4_alt = append_random_data(local_repo.as_ref(), &dataset_alias_2).await;

    assert_matches!(
        sync_svc.sync(&dataset_alias.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
        }) if old_head.as_ref() == Some(&b3) && new_head == b5
    );

    assert_matches!(
        sync_svc.sync(&dataset_alias_2.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DatasetsDiverged(DatasetsDivergedError { src_head, dst_head, uncommon_blocks_in_src, uncommon_blocks_in_dst }))
        if src_head == b4_alt && dst_head == b5 && uncommon_blocks_in_src == 1 && uncommon_blocks_in_dst == 2
    );
}

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
    )
    .await;
}

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_sync_to_from_s3() {
    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);

    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let bucket = "test-bucket";
    std::fs::create_dir(tmp_repo_dir.path().join(bucket)).unwrap();

    let minio = MinioServer::new(tmp_repo_dir.path(), access_key, secret_key).await;

    let repo_url = Url::from_str(&format!(
        "s3+http://{}:{}/{}",
        minio.address, minio.host_port, bucket
    ))
    .unwrap();

    do_test_sync(
        tmp_workspace_dir.path(),
        &DatasetRefRemote::from(&repo_url),
        &DatasetRefRemote::from(&repo_url),
        None,
    )
    .await;
}

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
    )
    .await;
}

#[test_log::test(tokio::test)]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
async fn test_sync_to_from_ipfs() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();

    let ipfs_daemon = IpfsDaemon::new().await;
    let ipfs_client = ipfs_daemon.client();
    let key_id = ipfs_client.key_gen("test").await.unwrap();
    let ipns_url = Url::from_str(&format!("ipns://{}", key_id)).unwrap();

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
    )
    .await;
}
