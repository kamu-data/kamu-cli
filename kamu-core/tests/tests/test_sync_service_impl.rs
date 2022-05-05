// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::utils::{HttpFileServer, MinioServer};
use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

use std::assert_matches::assert_matches;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

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
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let dataset_1_layout = DatasetLayout::new(&volume_layout, dataset_name_1);
    let dataset_2_layout = DatasetLayout::new(&volume_layout, dataset_name_2);

    let meta_dir_1 = workspace_layout.datasets_dir.join(dataset_name_1);
    let meta_dir_2 = workspace_layout.datasets_dir.join(dataset_name_2);

    let blocks_dir_1 = meta_dir_1.join("blocks");
    let blocks_dir_2 = meta_dir_2.join("blocks");

    let refs_dir_1 = meta_dir_1.join("refs");
    let refs_dir_2 = meta_dir_2.join("refs");

    assert_eq!(list_files(&blocks_dir_1), list_files(&blocks_dir_2));
    assert_eq!(
        list_files(&dataset_1_layout.data_dir),
        list_files(&dataset_2_layout.data_dir)
    );
    assert_eq!(
        list_files(&dataset_1_layout.checkpoints_dir),
        list_files(&dataset_2_layout.checkpoints_dir),
    );

    let head_1 = std::fs::read_to_string(refs_dir_1.join("head")).unwrap();
    let head_2 = std::fs::read_to_string(refs_dir_2.join("head")).unwrap();
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

async fn do_test_sync(tmp_workspace_dir: &Path, push_repo_url: Url, pull_repo_url: Url) {
    // Tests sync between "foo" -> remote -> "bar"
    let dataset_name = DatasetName::new_unchecked("foo");
    let dataset_name_2 = DatasetName::new_unchecked("bar");

    let workspace_layout = Arc::new(WorkspaceLayout::create(tmp_workspace_dir).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let dataset_layout = DatasetLayout::new(&volume_layout, &dataset_name);
    let dataset_layout_2 = DatasetLayout::new(&volume_layout, &dataset_name_2);
    let dataset_reg = Arc::new(DatasetRegistryImpl::new(workspace_layout.clone()));
    let remote_repo_reg = Arc::new(RemoteRepositoryRegistryImpl::new(workspace_layout.clone()));
    let local_repo = Arc::new(LocalDatasetRepositoryImpl::new(workspace_layout.clone()));

    let sync_svc = SyncServiceImpl::new(remote_repo_reg.clone(), local_repo);

    // Add repositories
    let push_repo_name = RepositoryName::new_unchecked("remote-push");
    let pull_repo_name = RepositoryName::new_unchecked("remote-pull");
    let push_remote_dataset_name =
        RemoteDatasetName::new(push_repo_name.clone(), None, dataset_name.clone());
    let pull_remote_dataset_name =
        RemoteDatasetName::new(pull_repo_name.clone(), None, dataset_name.clone());
    remote_repo_reg
        .add_repository(&push_repo_name, push_repo_url.clone())
        .unwrap();
    remote_repo_reg
        .add_repository(&pull_repo_name, pull_repo_url.clone())
        .unwrap();

    // Dataset does not exist locally / remotely //////////////////////////////
    assert_matches!(
        sync_svc
            .sync(
                &dataset_name.as_any_ref(),
                &push_remote_dataset_name.as_any_ref(),
                SyncOptions::default(),
                None,
            )
            .await,
        Err(SyncError::DatasetNotFound(e))
        if e.dataset_ref == dataset_name.as_any_ref()
    );

    assert_matches!(
        sync_svc
            .sync(
                &pull_remote_dataset_name.as_any_ref(),
                &dataset_name_2.as_any_ref(),
                SyncOptions::default(),
                None,
            )
            .await,
        Err(SyncError::DatasetNotFound(e))
        if e.dataset_ref == pull_remote_dataset_name.as_any_ref()
    );

    // Add dataset
    let snapshot = MetadataFactory::dataset_snapshot()
        .name(&dataset_name)
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let (_, b1) = dataset_reg.add_dataset(snapshot).unwrap();

    // Initial sync ///////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc.sync(&dataset_name.as_any_ref(), &push_remote_dataset_name.as_any_ref(),  SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 2,
        }) if new_head == b1
    );

    assert_matches!(
        sync_svc.sync(&pull_remote_dataset_name.as_any_ref(), &dataset_name_2.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 2,
        }) if new_head == b1
    );

    assert_in_sync(&workspace_layout, &dataset_name, &dataset_name_2);

    // Subsequent sync ////////////////////////////////////////////////////////
    let b2 = dataset_reg
        .get_metadata_chain(&dataset_name.as_local_ref())
        .unwrap()
        .append(
            MetadataFactory::metadata_block(create_random_data(&dataset_layout).await.build())
                .prev(&b1)
                .build(),
        );

    let b3 = dataset_reg
        .get_metadata_chain(&dataset_name.as_local_ref())
        .unwrap()
        .append(
            MetadataFactory::metadata_block(create_random_data(&dataset_layout).await.build())
                .prev(&b2)
                .build(),
        );

    assert_matches!(
        sync_svc.sync(&pull_remote_dataset_name.as_any_ref(), &dataset_name.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DatasetsDiverged(DatasetsDivergedError { src_head, dst_head }))
        if src_head == b1 && dst_head == b3
    );

    assert_matches!(
        sync_svc.sync(&dataset_name.as_any_ref(), &push_remote_dataset_name.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
        }) if old_head == Some(b1.clone()) && new_head == b3
    );

    assert_matches!(
        sync_svc.sync(&pull_remote_dataset_name.as_any_ref(), &dataset_name_2.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
        }) if old_head == Some(b1.clone()) && new_head == b3
    );

    assert_in_sync(&workspace_layout, &dataset_name, &dataset_name_2);

    // Up to date /////////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc
            .sync(
                &dataset_name.as_any_ref(),
                &push_remote_dataset_name.as_any_ref(),
                SyncOptions::default(),
                None
            )
            .await,
        Ok(SyncResult::UpToDate)
    );

    assert_matches!(
        sync_svc
            .sync(
                &pull_remote_dataset_name.as_any_ref(),
                &dataset_name_2.as_any_ref(),
                SyncOptions::default(),
                None
            )
            .await,
        Ok(SyncResult::UpToDate)
    );

    assert_in_sync(&workspace_layout, &dataset_name, &dataset_name_2);

    // Datasets diverged on push //////////////////////////////////////////////

    // Push a new block into dataset_2 (which we were pulling into before)
    let diverged_head = dataset_reg
        .get_metadata_chain(&dataset_name_2.as_local_ref())
        .unwrap()
        .append(
            MetadataFactory::metadata_block(create_random_data(&dataset_layout_2).await.build())
                .prev(&b3)
                .build(),
        );

    assert_matches!(
        sync_svc.sync(&dataset_name_2.into(), &push_remote_dataset_name.as_any_ref(), SyncOptions::default(), None).await,
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 1,
        }) if old_head == Some(b3.clone()) && new_head == diverged_head
    );

    // Try push from dataset_1
    assert_matches!(
        sync_svc.sync(&dataset_name.into(), &push_remote_dataset_name.as_any_ref(), SyncOptions::default(), None).await,
        Err(SyncError::DatasetsDiverged (DatasetsDivergedError { src_head, dst_head }))
        if src_head == b3 && dst_head == diverged_head
    );
}

#[test_log::test(tokio::test)]
async fn test_sync_to_from_local_fs() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    do_test_sync(tmp_workspace_dir.path(), repo_url.clone(), repo_url).await;
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

    do_test_sync(tmp_workspace_dir.path(), repo_url.clone(), repo_url).await;
}

#[test_log::test(tokio::test)]
async fn test_sync_from_http() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let push_repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    let server = HttpFileServer::new(tmp_repo_dir.path());
    let pull_repo_url = Url::from_str(&format!("http://{}/", server.local_addr())).unwrap();

    let _server_hdl = tokio::spawn(server.run());

    do_test_sync(tmp_workspace_dir.path(), push_repo_url, pull_repo_url).await;
}
