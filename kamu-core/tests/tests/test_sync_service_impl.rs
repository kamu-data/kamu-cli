// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::utils::MinioServer;
use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;
use opendatafabric::*;

use digest::Digest;
use std::assert_matches::assert_matches;
use std::path::{Path, PathBuf};
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
    dataset_id_1: &DatasetID,
    dataset_id_2: &DatasetID,
) {
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);

    let dataset_1_layout = DatasetLayout::new(&volume_layout, dataset_id_1);
    let dataset_2_layout = DatasetLayout::new(&volume_layout, dataset_id_2);

    let meta_dir_1 = workspace_layout.datasets_dir.join(dataset_id_1);
    let meta_dir_2 = workspace_layout.datasets_dir.join(dataset_id_2);

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

fn create_fake_data_file(dataset_layout: &DatasetLayout) -> PathBuf {
    use rand::RngCore;

    let mut hash = Sha3_256::zero();
    rand::thread_rng().fill_bytes(hash.as_array_mut());

    std::fs::create_dir_all(&dataset_layout.data_dir).unwrap();
    let path = dataset_layout.data_dir.join(hash.to_string());
    std::fs::write(&path, "<data>".as_bytes()).unwrap();
    path
}

fn do_test_sync(tmp_workspace_dir: &Path, repo_url: Url) {
    // Tests sync between "foo" -> remote -> "bar"
    let dataset_id = DatasetID::new_unchecked("foo");
    let dataset_id_2 = DatasetID::new_unchecked("bar");

    let workspace_layout = Arc::new(WorkspaceLayout::create(tmp_workspace_dir).unwrap());
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let dataset_layout = DatasetLayout::new(&volume_layout, dataset_id);
    let metadata_repo = Arc::new(MetadataRepositoryImpl::new(workspace_layout.clone()));
    let repository_factory = Arc::new(RepositoryFactory::new());

    let sync_svc = SyncServiceImpl::new(
        workspace_layout.clone(),
        metadata_repo.clone(),
        repository_factory.clone(),
    );

    // Add repository
    let repo_id = RepositoryID::new_unchecked("remote");
    let remote_dataset_ref = DatasetRefBuf::new(Some(repo_id), None, dataset_id);
    metadata_repo.add_repository(&repo_id, repo_url).unwrap();

    // Dataset does not exist locally / remotely //////////////////////////////
    assert_matches!(
        sync_svc.sync_to(
            dataset_id,
            &remote_dataset_ref,
            SyncOptions::default(),
            None,
        ),
        Err(SyncError::LocalDatasetDoesNotExist { .. })
    );

    assert_matches!(
        sync_svc.sync_from(
            &remote_dataset_ref,
            dataset_id_2,
            SyncOptions::default(),
            None,
        ),
        Err(SyncError::RemoteDatasetDoesNotExist { .. })
    );

    // Add dataset
    let snapshot = MetadataFactory::dataset_snapshot()
        .id(&dataset_id)
        .source(MetadataFactory::dataset_source_root().build())
        .build();

    let b1 = metadata_repo.add_dataset(snapshot).unwrap();

    // Initial sync ///////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc.sync_to(dataset_id, &remote_dataset_ref,  SyncOptions::default(), None),
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 1,
        }) if new_head == b1
    );

    assert_matches!(
        sync_svc.sync_from(&remote_dataset_ref, dataset_id_2, SyncOptions::default(), None),
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 1,
        }) if new_head == b1
    );

    assert_in_sync(&workspace_layout, dataset_id, dataset_id_2);

    // Subsequent sync ////////////////////////////////////////////////////////
    create_fake_data_file(&dataset_layout);
    let b2 = metadata_repo
        .get_metadata_chain(dataset_id)
        .unwrap()
        .append(
            MetadataFactory::metadata_block()
                .prev(&b1)
                .output_slice(OutputSlice {
                    data_logical_hash: Multihash::new(
                        MulticodecCode::Sha3_256,
                        &sha3::Sha3_256::digest(b"foo"),
                    ),
                    data_physical_hash: Multihash::new(
                        MulticodecCode::Sha3_256,
                        &sha3::Sha3_256::digest(b"bar"),
                    ),
                    data_interval: OffsetInterval { start: 0, end: 9 },
                })
                .build(),
        );

    create_fake_data_file(&dataset_layout);
    let b3 = metadata_repo
        .get_metadata_chain(dataset_id)
        .unwrap()
        .append(
            MetadataFactory::metadata_block()
                .prev(&b2)
                .output_slice(OutputSlice {
                    data_logical_hash: Multihash::new(
                        MulticodecCode::Sha3_256,
                        &sha3::Sha3_256::digest(b"foo"),
                    ),
                    data_physical_hash: Multihash::new(
                        MulticodecCode::Sha3_256,
                        &sha3::Sha3_256::digest(b"bar"),
                    ),
                    data_interval: OffsetInterval { start: 10, end: 29 },
                })
                .build(),
        );

    let checkpoint_dir = dataset_layout.checkpoints_dir.join(b3.to_string());
    std::fs::create_dir_all(&checkpoint_dir).unwrap();
    std::fs::write(
        &checkpoint_dir.join("checkpoint_data.bin"),
        "<data>".as_bytes(),
    )
    .unwrap();

    assert_matches!(
        sync_svc.sync_from(&remote_dataset_ref, dataset_id, SyncOptions::default(), None),
        Err(SyncError::DatasetsDiverged { local_head, remote_head})
        if local_head == b3 && remote_head == b1
    );

    assert_matches!(
        sync_svc.sync_to(dataset_id, &remote_dataset_ref, SyncOptions::default(), None),
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
        }) if old_head == Some(b1) && new_head == b3
    );

    assert_matches!(
        sync_svc.sync_from(&remote_dataset_ref, dataset_id_2, SyncOptions::default(), None),
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
        }) if old_head == Some(b1) && new_head == b3
    );

    assert_in_sync(&workspace_layout, dataset_id, dataset_id_2);

    // Up to date /////////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc.sync_to(
            dataset_id,
            &remote_dataset_ref,
            SyncOptions::default(),
            None
        ),
        Ok(SyncResult::UpToDate)
    );

    assert_matches!(
        sync_svc.sync_from(
            &remote_dataset_ref,
            dataset_id_2,
            SyncOptions::default(),
            None
        ),
        Ok(SyncResult::UpToDate)
    );

    assert_in_sync(&workspace_layout, dataset_id, dataset_id_2);

    // Datasets diverged on push //////////////////////////////////////////////

    // Push a new block into dataset_2 (which we were pulling into before)
    let diverged_head = metadata_repo
        .get_metadata_chain(dataset_id_2)
        .unwrap()
        .append(
            MetadataFactory::metadata_block()
                .prev(&b3)
                .output_slice(OutputSlice {
                    data_logical_hash: Multihash::new(
                        MulticodecCode::Sha3_256,
                        &sha3::Sha3_256::digest(b"foo"),
                    ),
                    data_physical_hash: Multihash::new(
                        MulticodecCode::Sha3_256,
                        &sha3::Sha3_256::digest(b"bar"),
                    ),
                    data_interval: OffsetInterval { start: 30, end: 49 },
                })
                .build(),
        );

    assert_matches!(
        sync_svc.sync_to(dataset_id_2, &remote_dataset_ref, SyncOptions::default(), None),
        Ok(SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 1,
        }) if old_head == Some(b3) && new_head == diverged_head
    );

    // Try push from dataset_1
    assert_matches!(
        sync_svc.sync_to(dataset_id, &remote_dataset_ref, SyncOptions::default(), None),
        Err(SyncError::DatasetsDiverged { local_head, remote_head })
        if local_head == b3 && remote_head == diverged_head
    );
}

#[test]
fn test_sync_to_from_local_fs() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    do_test_sync(tmp_workspace_dir.path(), repo_url);
}

#[test]
#[cfg_attr(feature = "skip_docker_tests", ignore)]
fn test_sync_to_from_s3() {
    let access_key = "AKIAIOSFODNN7EXAMPLE";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
    std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);

    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let bucket = "test-bucket";
    std::fs::create_dir(tmp_repo_dir.path().join(bucket)).unwrap();

    let minio = MinioServer::new(tmp_repo_dir.path(), access_key, secret_key);

    use std::str::FromStr;
    let repo_url = Url::from_str(&format!(
        "s3+http://{}:{}/{}",
        minio.address, minio.host_port, bucket
    ))
    .unwrap();

    do_test_sync(tmp_workspace_dir.path(), repo_url);
}
