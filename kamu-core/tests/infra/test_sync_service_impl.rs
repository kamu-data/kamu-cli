use kamu::domain::*;
use kamu::infra::*;
use kamu_test::*;
use opendatafabric::*;

use assert_matches::assert_matches;
use chrono::prelude::*;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
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
    local_dataset_id: &DatasetID,
    remote_dataset_id: &DatasetID,
    remote_dir: &Path,
) {
    let local_volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let local_dataset_layout = DatasetLayout::new(&local_volume_layout, local_dataset_id);
    let local_meta_dir = workspace_layout.datasets_dir.join(local_dataset_id);
    let local_blocks_dir = local_meta_dir.join("blocks");
    let local_refs_dir = local_meta_dir.join("refs");
    let local_checkpoint_dir = local_dataset_layout.checkpoints_dir;
    let local_data_dir = local_dataset_layout.data_dir;

    let remote_dataset_dir = remote_dir.join(remote_dataset_id);
    let remote_meta_dir = remote_dataset_dir.join("meta");
    let remote_blocks_dir = remote_meta_dir.join("blocks");
    let remote_refs_dir = remote_meta_dir.join("refs");
    let remote_checkpoint_dir = remote_dataset_dir.join("checkpoint");
    let remote_data_dir = remote_dataset_dir.join("data");

    assert_eq!(
        list_files(&local_blocks_dir),
        list_files(&remote_blocks_dir)
    );

    assert_eq!(list_files(&local_data_dir), list_files(&remote_data_dir));

    assert_eq!(
        list_files(&local_checkpoint_dir),
        list_files(&remote_checkpoint_dir),
    );

    let local_head = std::fs::read_to_string(local_refs_dir.join("head")).unwrap();
    let remote_head = std::fs::read_to_string(remote_refs_dir.join("head")).unwrap();
    assert_eq!(local_head, remote_head);
}

fn create_fake_data_file(dataset_layout: &DatasetLayout) -> PathBuf {
    let t = Utc::now();
    let file_name = format!(
        "{}.snappy.parquet",
        t.to_rfc3339_opts(SecondsFormat::Nanos, true),
    );

    std::fs::create_dir_all(&dataset_layout.data_dir).unwrap();

    let path = dataset_layout.data_dir.join(file_name);
    std::fs::write(&path, "<data>".as_bytes()).unwrap();
    path
}

#[test]
fn test_sync_to_local_fs() {
    // Tests sync between "foo" -> remote -> "bar"

    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_remote_dir = tempfile::tempdir().unwrap();

    let dataset_id = DatasetID::new_unchecked("foo");
    let dataset_id_2 = DatasetID::new_unchecked("bar");

    let logger = slog::Logger::root(slog::Discard, slog::o!());
    let workspace_layout = WorkspaceLayout::create(tmp_workspace_dir.path()).unwrap();
    let volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let dataset_layout = DatasetLayout::new(&volume_layout, dataset_id);
    let metadata_repo = Rc::new(RefCell::new(MetadataRepositoryImpl::new(&workspace_layout)));
    let remote_factory = Arc::new(Mutex::new(RemoteFactory::new(logger.clone())));

    let mut sync_svc = SyncServiceImpl::new(
        workspace_layout.clone(),
        metadata_repo.clone(),
        remote_factory.clone(),
        logger.clone(),
    );

    // Add remote
    let remote_id = String::from("remote");
    let remote_url = Url::from_directory_path(tmp_remote_dir.path()).unwrap();
    metadata_repo
        .borrow_mut()
        .add_remote(&remote_id, remote_url)
        .unwrap();

    // Dataset does not exist locally / remotely //////////////////////////////
    assert_matches!(
        sync_svc.sync_to(
            dataset_id,
            dataset_id,
            &remote_id,
            SyncOptions::default(),
            None,
        ),
        Err(SyncError::LocalDatasetDoesNotExist { .. })
    );

    assert_matches!(
        sync_svc.sync_from(
            dataset_id_2,
            dataset_id,
            &remote_id,
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

    let b1 = metadata_repo.borrow_mut().add_dataset(snapshot).unwrap();

    // Initial sync ///////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc.sync_to(dataset_id, dataset_id, &remote_id, SyncOptions::default(), None),
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
        }) if new_head == b1
    );
    assert_in_sync(
        &workspace_layout,
        dataset_id,
        dataset_id,
        tmp_remote_dir.path(),
    );

    assert_matches!(
        sync_svc.sync_from(dataset_id_2, dataset_id, &remote_id, SyncOptions::default(), None),
        Ok(SyncResult::Updated {
            old_head: None,
            new_head,
        }) if new_head == b1
    );
    assert_in_sync(
        &workspace_layout,
        dataset_id_2,
        dataset_id,
        tmp_remote_dir.path(),
    );

    // Subsequent sync ////////////////////////////////////////////////////////
    create_fake_data_file(&dataset_layout);
    let b2 = metadata_repo
        .borrow_mut()
        .get_metadata_chain(dataset_id)
        .unwrap()
        .append(
            MetadataFactory::metadata_block()
                .prev(&b1)
                .output_slice(DataSlice {
                    hash: Sha3_256::zero(),
                    interval: TimeInterval::singleton(Utc::now()),
                    num_records: 10,
                })
                .build(),
        );

    create_fake_data_file(&dataset_layout);
    let b3 = metadata_repo
        .borrow_mut()
        .get_metadata_chain(dataset_id)
        .unwrap()
        .append(
            MetadataFactory::metadata_block()
                .prev(&b2)
                .output_slice(DataSlice {
                    hash: Sha3_256::zero(),
                    interval: TimeInterval::singleton(Utc::now()),
                    num_records: 20,
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
        sync_svc.sync_from(dataset_id, dataset_id, &remote_id, SyncOptions::default(), None),
        Err(SyncError::DatasetsDiverged { local_head, remote_head})
        if local_head == b3 && remote_head == b1
    );

    assert_matches!(
        sync_svc.sync_to(dataset_id, dataset_id, &remote_id, SyncOptions::default(), None),
        Ok(SyncResult::Updated {
            old_head,
            new_head,
        }) if old_head == Some(b1) && new_head == b3
    );
    assert_in_sync(
        &workspace_layout,
        dataset_id,
        dataset_id,
        tmp_remote_dir.path(),
    );

    assert_matches!(
        sync_svc.sync_from(dataset_id_2, dataset_id, &remote_id, SyncOptions::default(), None),
        Ok(SyncResult::Updated {
            old_head,
            new_head,
        }) if old_head == Some(b1) && new_head == b3
    );
    assert_in_sync(
        &workspace_layout,
        dataset_id_2,
        dataset_id,
        tmp_remote_dir.path(),
    );

    // Up to date /////////////////////////////////////////////////////////////
    assert_matches!(
        sync_svc.sync_to(
            dataset_id,
            dataset_id,
            &remote_id,
            SyncOptions::default(),
            None
        ),
        Ok(SyncResult::UpToDate)
    );
    assert_in_sync(
        &workspace_layout,
        dataset_id,
        dataset_id,
        tmp_remote_dir.path(),
    );

    assert_matches!(
        sync_svc.sync_from(
            dataset_id_2,
            dataset_id,
            &remote_id,
            SyncOptions::default(),
            None
        ),
        Ok(SyncResult::UpToDate)
    );
    assert_in_sync(
        &workspace_layout,
        dataset_id_2,
        dataset_id,
        tmp_remote_dir.path(),
    );

    // Datasets diverged on push //////////////////////////////////////////////
    let mut remote_chain =
        MetadataChainImpl::new(&tmp_remote_dir.path().join(dataset_id).join("meta"));
    let diverged_head = remote_chain.append(MetadataFactory::metadata_block().prev(&b3).build());

    assert_matches!(
        sync_svc.sync_to(dataset_id, dataset_id, &remote_id, SyncOptions::default(), None),
        Err(SyncError::DatasetsDiverged { local_head, remote_head })
        if local_head == b3 && remote_head == diverged_head
    );
}
