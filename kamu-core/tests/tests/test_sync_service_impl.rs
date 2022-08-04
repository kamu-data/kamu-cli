// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::utils::{HttpFileServer, IpfsDaemon, MinioServer};
use kamu::domain::*;
use kamu::infra::utils::ipfs_wrapper::IpfsClient;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

use std::assert_matches::assert_matches;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

async fn append_block(
    local_repo: &dyn LocalDatasetRepository,
    dataset_ref: impl Into<DatasetRefLocal>,
    block: MetadataBlock,
) -> Multihash {
    let ds = local_repo.get_dataset(&dataset_ref.into()).await.unwrap();
    ds.as_metadata_chain()
        .append(block, AppendOpts::default())
        .await
        .unwrap()
}

fn list_files(dir: &Path) -> Vec<PathBuf> {
    if !dir.exists() {
        return Vec::new();
    }

    let mut v = _list_files_rec(dir);

    for path in v.iter_mut() {
        *path = path.strip_prefix(dir).unwrap().to_owned();
    }

    v.sort();
    v
}

fn _list_files_rec(dir: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .flat_map(|e| {
            let entry = e.unwrap();
            let path = entry.path();
            if path.is_dir() {
                _list_files_rec(&path)
            } else {
                vec![path]
            }
        })
        .collect()
}

fn assert_in_sync(
    workspace_layout: &WorkspaceLayout,
    dataset_name_1: &DatasetName,
    dataset_name_2: &DatasetName,
) {
    let dataset_1_layout = workspace_layout.dataset_layout(dataset_name_1);
    let dataset_2_layout = workspace_layout.dataset_layout(dataset_name_2);

    assert_eq!(
        list_files(&dataset_1_layout.blocks_dir),
        list_files(&dataset_2_layout.blocks_dir)
    );
    assert_eq!(
        list_files(&dataset_1_layout.refs_dir),
        list_files(&dataset_2_layout.refs_dir)
    );
    assert_eq!(
        list_files(&dataset_1_layout.data_dir),
        list_files(&dataset_2_layout.data_dir)
    );
    assert_eq!(
        list_files(&dataset_1_layout.checkpoints_dir),
        list_files(&dataset_2_layout.checkpoints_dir)
    );

    let head_1 = std::fs::read_to_string(dataset_1_layout.refs_dir.join("head")).unwrap();
    let head_2 = std::fs::read_to_string(dataset_2_layout.refs_dir.join("head")).unwrap();
    assert_eq!(head_1, head_2);
}

async fn create_random_file(root: &Path) -> (Multihash, usize) {
    use rand::RngCore;

    let mut data = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut data);

    std::fs::create_dir_all(root).unwrap();

    let repo = ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(root);
    let hash = repo
        .insert_bytes(&data, InsertOpts::default())
        .await
        .unwrap()
        .hash;

    (hash, data.len())
}

async fn create_random_data(dataset_layout: &DatasetLayout) -> AddDataBuilder {
    let (d_hash, d_size) = create_random_file(&dataset_layout.data_dir).await;
    let (c_hash, c_size) = create_random_file(&dataset_layout.checkpoints_dir).await;
    MetadataFactory::add_data()
        .data_physical_hash(d_hash)
        .data_size(d_size as i64)
        .checkpoint_physical_hash(c_hash)
        .checkpoint_size(c_size as i64)
}

async fn do_test_sync(
    tmp_workspace_dir: &Path,
    push_ref: &DatasetRefRemote,
    pull_ref: &DatasetRefRemote,
    ipfs: Option<(IpfsGateway, IpfsClient)>,
) {
    // Tests sync between "foo" -> remote -> "bar"
    let dataset_name = DatasetName::new_unchecked("foo");
    let dataset_name_2 = DatasetName::new_unchecked("bar");

    let workspace_layout = Arc::new(WorkspaceLayout::create(tmp_workspace_dir).unwrap());
    let dataset_layout = workspace_layout.dataset_layout(&dataset_name);
    let dataset_layout_2 = workspace_layout.dataset_layout(&dataset_name_2);
    let local_repo = Arc::new(LocalDatasetRepositoryImpl::new(workspace_layout.clone()));
    let remote_repo_reg = Arc::new(RemoteRepositoryRegistryImpl::new(workspace_layout.clone()));
    let dataset_factory = Arc::new(DatasetFactoryImpl::new());
    let (ipfs_gateway, ipfs_client) = ipfs.unwrap_or_default();

    let sync_svc = SyncServiceImpl::new(
        remote_repo_reg.clone(),
        local_repo.clone(),
        dataset_factory,
        Arc::new(ipfs_client),
        ipfs_gateway,
    );

    // Dataset does not exist locally / remotely //////////////////////////////
    assert_matches!(
        sync_svc
            .sync(
                &dataset_name.as_any_ref(),
                &push_ref.as_any_ref(),
                SyncOptions::default(),
                None,
            )
            .await,
        Err(SyncError::DatasetNotFound(e)) if e.dataset_ref == dataset_name.as_any_ref()
    );

    assert_matches!(
        sync_svc
            .sync(
                &pull_ref.as_any_ref(),
                &dataset_name_2.as_any_ref(),
                SyncOptions::default(),
                None,
            )
            .await,
        Err(SyncError::DatasetNotFound(e)) if e.dataset_ref == pull_ref.as_any_ref()
    );

    // Add dataset
    let snapshot = MetadataFactory::dataset_snapshot()
        .name(&dataset_name)
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result = local_repo
        .create_dataset_from_snapshot(snapshot)
        .await
        .unwrap();
    let b1 = create_result.head;
    let b1_sequence_number = create_result.head_sequence_number;

    // Initial sync ///////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc.sync(
            &dataset_name.as_any_ref(),
            &push_ref.as_any_ref(),
            SyncOptions { create_if_not_exists: false, ..Default::default() },
            None
        ).await,
        Err(SyncError::DatasetNotFound(e)) if e.dataset_ref == push_ref.as_any_ref()
    );

    assert_matches!(
        sync_svc.sync(&dataset_name.as_any_ref(), &push_ref.as_any_ref(),  SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 2,
        }) if new_head == b1
    );

    assert_matches!(
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_name_2.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 2,
        }) if new_head == b1
    );

    assert_in_sync(&workspace_layout, &dataset_name, &dataset_name_2);

    // Subsequent sync ////////////////////////////////////////////////////////
    let b2 = append_block(
        local_repo.as_ref(),
        &dataset_name,
        MetadataFactory::metadata_block(create_random_data(&dataset_layout).await.build())
            .prev(&b1, b1_sequence_number)
            .build(),
    )
    .await;

    let b3 = append_block(
        local_repo.as_ref(),
        &dataset_name,
        MetadataFactory::metadata_block(create_random_data(&dataset_layout).await.build())
            .prev(&b2, b1_sequence_number + 1)
            .build(),
    )
    .await;

    assert_matches!(
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_name.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DestinationAhead(DestinationAheadError {src_head, dst_head, dst_ahead_size: 2 }))
        if src_head == b1 && dst_head == b3
    );

    assert_matches!(
        sync_svc.sync(&dataset_name.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
        }) if old_head.as_ref() == Some(&b1) && new_head == b3
    );

    assert_matches!(
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_name_2.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
        }) if old_head.as_ref() == Some(&b1) && new_head == b3
    );

    assert_in_sync(&workspace_layout, &dataset_name, &dataset_name_2);

    // Up to date /////////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc
            .sync(
                &dataset_name.as_any_ref(),
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
                &dataset_name_2.as_any_ref(),
                SyncOptions::default(),
                None
            )
            .await,
        Ok(SyncResult::UpToDate)
    );

    assert_in_sync(&workspace_layout, &dataset_name, &dataset_name_2);

    // Datasets out-of-sync on push //////////////////////////////////////////////

    // Push a new block into dataset_2 (which we were pulling into before)
    let exta_head = append_block(
        local_repo.as_ref(),
        &dataset_name_2,
        MetadataFactory::metadata_block(create_random_data(&dataset_layout_2).await.build())
            .prev(&b3, b1_sequence_number + 2)
            .build(),
    )
    .await;

    assert_matches!(
        sync_svc.sync(&dataset_name_2.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 1,
        }) if old_head == Some(b3.clone()) && new_head == exta_head
    );

    // Try push from dataset_1
    assert_matches!(
        sync_svc.sync(&dataset_name.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DestinationAhead(DestinationAheadError { src_head, dst_head, dst_ahead_size: 1 }))
        if src_head == b3 && dst_head == exta_head
    );

    // Try push from dataset_1 with --force: it should abandon the diverged_head block
    assert_matches!(
        sync_svc
            .sync(
                &dataset_name.as_any_ref(),
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
        sync_svc.sync(&pull_ref.as_any_ref(), &dataset_name_2.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DestinationAhead(DestinationAheadError { src_head, dst_head, dst_ahead_size: 1}))
        if src_head == b3 && dst_head == exta_head
    );

    // Try pulling dataset_2 with --force: should abandon diverged_head
    assert_matches!(
        sync_svc
            .sync(
                &pull_ref.as_any_ref(),
                &dataset_name_2.as_any_ref(),
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

    let b4 = append_block(
        local_repo.as_ref(),
        &dataset_name,
        MetadataFactory::metadata_block(create_random_data(&dataset_layout).await.build())
            .prev(&b3, b1_sequence_number + 2)
            .build(),
    )
    .await;

    let b5 = append_block(
        local_repo.as_ref(),
        &dataset_name,
        MetadataFactory::metadata_block(create_random_data(&dataset_layout).await.build())
            .prev(&b4, b1_sequence_number + 3)
            .build(),
    )
    .await;

    let b4_alt = append_block(
        local_repo.as_ref(),
        &dataset_name_2,
        MetadataFactory::metadata_block(create_random_data(&dataset_layout_2).await.build())
            .prev(&b3, b1_sequence_number + 2)
            .build(),
    )
    .await;

    assert_matches!(
        sync_svc.sync(&dataset_name.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
        }) if old_head.as_ref() == Some(&b3) && new_head == b5
    );

    assert_matches!(
        sync_svc.sync(&dataset_name_2.as_any_ref(), &push_ref.as_any_ref(), SyncOptions::default(), None).await,
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

    let minio = MinioServer::new(tmp_repo_dir.path(), access_key, secret_key);

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

    let ipfs_daemon = IpfsDaemon::new();
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
