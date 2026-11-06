// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use kamu::domain::*;
use kamu::testing::*;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocolConfig;
use kamu::*;
use kamu_auth_rebac::RebacService;
use kamu_datasets::*;
use kamu_datasets_services::*;
use odf::dataset::DatasetStorageUnit as _;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::AsTypedBlock as _;
use odf::metadata::testing::MetadataFactory;
use test_utils::LocalS3Server;
use time_source::SystemTimeSource;
use url::Url;

use crate::utils::SyncWorkspaceCatalog;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_first_pull_creates_dataset_at_transferred_head_local_fs() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap())
        .assert_first_pull_creates_dataset_at_transferred_head()
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_first_pull_creates_dataset_at_transferred_head_s3() {
    let s3 = LocalS3Server::new().await;
    s3.set_credentials_env_vars();

    LocalPullHarness::new(s3.url.clone())
        .assert_first_pull_creates_dataset_at_transferred_head()
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transfer_into_existing_dataset_moves_no_head() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    harness.create_source_dataset().await;
    harness.push_source().await;
    harness.pull(SyncOptions::default(), None).await.unwrap();
    let old_head = harness.destination_head().await.unwrap();

    let new_head = harness.append_source_data().await;
    harness.push_source().await;

    let transferred = harness.transfer().await;
    assert_eq!(harness.destination_head().await, Some(old_head.clone()));

    assert_eq!(
        harness.commit(transferred).await.unwrap(),
        SyncResult::Updated {
            old_head: Some(old_head),
            new_head: new_head.clone(),
            num_blocks: 1,
            increment: odf::dataset::MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 10,
                updated_watermark: None,
            },
        }
    );
    assert_eq!(harness.destination_head().await, Some(new_head));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_first_pull_continues_after_uncommitted_transfer_local_fs() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap())
        .assert_first_pull_continues_after_uncommitted_transfer()
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_first_pull_continues_after_uncommitted_transfer_s3() {
    let s3 = LocalS3Server::new().await;
    s3.set_credentials_env_vars();

    LocalPullHarness::new(s3.url.clone())
        .assert_first_pull_continues_after_uncommitted_transfer()
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_retry_reuses_objects_of_an_uncommitted_transfer() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap())
        .assert_retry_reuses_objects_of_uncommitted_transfer(tmp_repo_dir.path())
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Zero lifts the limit on parallel transfers
#[test_log::test(tokio::test)]
async fn test_pull_with_zero_max_parallel_transfers() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::with_max_parallel_transfers(
        Url::from_directory_path(tmp_repo_dir.path()).unwrap(),
        0,
    );

    harness.create_source_dataset().await;
    let source_head = harness.append_source_data().await;
    harness.push_source().await;

    assert_matches!(
        harness.pull(SyncOptions::default(), None).await,
        Ok(SyncResult::Updated { new_head, .. }) if new_head == source_head
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An update without data or checkpoint files leaves nothing to copy
#[test_log::test(tokio::test)]
async fn test_metadata_only_pull_with_zero_max_parallel_transfers() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::with_max_parallel_transfers(
        Url::from_directory_path(tmp_repo_dir.path()).unwrap(),
        0,
    );

    harness.create_source_dataset().await;
    harness.push_source().await;
    harness.pull(SyncOptions::default(), None).await.unwrap();
    let source_head = harness.append_source_info().await;
    harness.push_source().await;

    assert_matches!(
        harness.pull(SyncOptions::default(), None).await,
        Ok(SyncResult::Updated { new_head, num_blocks: 1, .. }) if new_head == source_head
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_validates_remote_blocks_by_default() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    harness.create_source_dataset().await;
    harness.push_source().await;
    harness.pull(SyncOptions::default(), None).await.unwrap();
    let old_head = harness.destination_head().await.unwrap();

    let source_head = harness.append_source_data().await;
    harness.push_source().await;
    harness.tamper_repository_block(|hash, _| hash == &source_head);

    assert_matches!(
        harness.pull(SyncOptions::default(), None).await,
        Err(SyncError::Corrupted(_))
    );
    assert_eq!(harness.destination_head().await, Some(old_head));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_first_pull_of_tampered_seed_creates_no_dataset() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    harness.create_source_seed_only().await;
    harness.push_source().await;
    harness.tamper_repository_block(|_, block| block.as_typed::<odf::metadata::Seed>().is_some());

    assert_matches!(
        harness.pull(SyncOptions::default(), None).await,
        Err(SyncError::Corrupted(_))
    );
    assert_eq!(harness.destination_head().await, None);
    assert!(!harness.destination_alias_resolves().await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_rejects_tampered_data_file() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap())
        .assert_pull_rejects_tampered_object(RepositoryObjects::Data)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_rejects_tampered_checkpoint_file() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap())
        .assert_pull_rejects_tampered_object(RepositoryObjects::Checkpoints)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_first_pull_creates_dataset_with_requested_visibility() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    harness.create_source_dataset().await;
    harness.push_source().await;

    harness
        .pull(
            SyncOptions {
                dataset_visibility: odf::DatasetVisibility::Public,
                ..SyncOptions::default()
            },
            None,
        )
        .await
        .unwrap();

    assert!(harness.destination_allows_public_read().await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_commit_into_dataset_deleted_during_transfer_fails() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    harness.create_source_dataset().await;
    harness.push_source().await;
    harness.pull(SyncOptions::default(), None).await.unwrap();
    harness.append_source_data().await;
    harness.push_source().await;

    let transferred = harness.transfer().await;
    harness.delete_destination().await;

    assert_matches!(
        harness.commit(transferred).await,
        Err(SyncError::DatasetNotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_forced_pull_replaces_diverged_history() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    harness.create_source_dataset().await;
    harness.push_source().await;
    harness.pull(SyncOptions::default(), None).await.unwrap();

    let diverged_head = harness.append_destination_data().await;
    let source_head = harness.append_source_data().await;
    harness.push_source().await;

    assert_matches!(
        harness.pull(SyncOptions::default(), None).await,
        Err(SyncError::DatasetsDiverged(_))
    );

    assert_matches!(
        harness.pull(SyncOptions { force: true, ..SyncOptions::default() }, None).await,
        Ok(SyncResult::Updated { old_head: Some(old_head), new_head, increment, .. })
            if old_head == diverged_head && new_head == source_head
                // The whole replacing chain counts
                && increment == odf::dataset::MetadataChainIncrementInterval {
                    num_blocks: 3,
                    num_records: 10,
                    updated_watermark: None,
                }
    );
    assert_eq!(harness.destination_head().await, Some(source_head));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_pull_with_head_moved_during_transfer_local_fs() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap())
        .assert_pull_keeps_head_moved_during_transfer(true)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_unforced_pull_with_head_moved_during_transfer_local_fs() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap())
        .assert_pull_keeps_head_moved_during_transfer(false)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_pull_with_head_moved_during_transfer_s3() {
    let s3 = LocalS3Server::new().await;
    s3.set_credentials_env_vars();

    LocalPullHarness::new(s3.url.clone())
        .assert_pull_keeps_head_moved_during_transfer(true)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_from_unreachable_repository_fails_recoverably() {
    // Nothing listens on loopback port 1 (tcpmux)
    let harness = LocalPullHarness::new(Url::parse("http://127.0.0.1:1/").unwrap());

    let result = harness.pull(SyncOptions::default(), None).await;

    assert_matches!(result, Err(e @ SyncError::Internal(_)) if e.is_recoverable());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_from_unknown_repository_fails_in_transfer_unrecoverably() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    let result = harness.pull_from("missing/foo".parse().unwrap()).await;

    assert_matches!(result, Err(e @ SyncError::RepositoryNotFound(_)) if !e.is_recoverable());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_of_remote_by_id_fails_unrecoverably() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    let result = harness
        .pull_from(odf::DatasetRefRemote::ID(
            None,
            odf::DatasetID::new_seeded_ed25519(b"foo"),
        ))
        .await;

    assert_matches!(result, Err(e @ SyncError::RemoteByIdUnsupported(_)) if !e.is_recoverable());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_first_pull_into_alias_taken_before_commit_fails_unrecoverably() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::with_destination_alias_taken(
        Url::from_directory_path(tmp_repo_dir.path()).unwrap(),
    );

    harness.create_source_dataset().await;
    harness.push_source().await;
    let transferred = harness.transfer().await;

    assert_matches!(
        harness.commit(transferred).await,
        Err(e @ SyncError::CreateDatasetFailed(CreateDatasetError::NameCollision(_)))
            if !e.is_recoverable()
    );
    assert_eq!(harness.destination_head().await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_concurrent_first_pulls_create_dataset_once() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    harness.create_source_dataset().await;
    let source_head = harness.append_source_data().await;
    harness.push_source().await;

    let first = harness.transfer().await;
    let second = harness.transfer().await;
    harness.commit(first).await.unwrap();

    assert_matches!(
        harness.commit(second).await,
        Err(e @ SyncError::CreateDatasetFailed(CreateDatasetError::RefCollision(_)))
            if e.is_recoverable()
    );
    assert_eq!(harness.destination_head().await, Some(source_head));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pull_into_headless_dataset_of_another_id_fails_unrecoverably() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let harness = LocalPullHarness::new(Url::from_directory_path(tmp_repo_dir.path()).unwrap());

    harness.create_source_dataset().await;
    harness.append_source_data().await;
    harness.push_source().await;

    let result = harness.transfer_into_headless_dataset("other").await;

    assert_matches!(
        result,
        Err(e @ SyncError::DatasetIdMismatch(_)) if !e.is_recoverable()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A source workspace that pushes to a repository, and a destination
/// workspace that pulls from it
struct LocalPullHarness {
    _tmp_dir: tempfile::TempDir,
    source_catalog: dill::Catalog,
    destination_catalog: dill::Catalog,
    repo_url: Url,
    dataset_alias: odf::DatasetAlias,
}

impl LocalPullHarness {
    fn new(repo_url: Url) -> Self {
        Self::build(repo_url, false, None)
    }

    /// Another dataset takes the destination's name when a first pull creates
    /// it
    fn with_destination_alias_taken(repo_url: Url) -> Self {
        Self::build(repo_url, true, None)
    }

    /// The destination copies at most `limit` files at a time
    fn with_max_parallel_transfers(repo_url: Url, limit: usize) -> Self {
        Self::build(
            repo_url,
            false,
            Some(SimpleTransferProtocolConfig {
                max_parallel_transfers: limit,
            }),
        )
    }

    fn build(
        repo_url: Url,
        destination_alias_taken: bool,
        maybe_destination_transfer_config: Option<SimpleTransferProtocolConfig>,
    ) -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();

        Self {
            source_catalog: Self::build_workspace_catalog(
                &tmp_dir.path().join("source"),
                false,
                None,
            ),
            destination_catalog: Self::build_workspace_catalog(
                &tmp_dir.path().join("destination"),
                destination_alias_taken,
                maybe_destination_transfer_config,
            ),
            _tmp_dir: tmp_dir,
            repo_url,
            dataset_alias: odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo")),
        }
    }

    fn build_workspace_catalog(
        workspace_dir: &Path,
        alias_taken: bool,
        maybe_transfer_config: Option<SimpleTransferProtocolConfig>,
    ) -> dill::Catalog {
        let mut dataset_entry_writer = MockDatasetEntryWriter::new();
        dataset_entry_writer
            .expect_create_entry()
            .returning(move |_, _, _, dataset_name, _| {
                if alias_taken {
                    Err(DatasetEntryNameCollisionError::new(dataset_name.clone()).into())
                } else {
                    Ok(())
                }
            });

        let workspace_catalog = SyncWorkspaceCatalog::new(workspace_dir)
            .with_dataset_entry_writer(dataset_entry_writer);
        match maybe_transfer_config {
            Some(config) => workspace_catalog.with_simple_transfer_config(config),
            None => workspace_catalog,
        }
        .build()
    }

    /// A first pull creates the dataset with its `HEAD` at the last
    /// transferred block in one commit
    async fn assert_first_pull_creates_dataset_at_transferred_head(&self) {
        self.create_source_dataset().await;
        let source_head = self.append_source_data().await;
        self.push_source().await;

        assert_eq!(
            self.pull(SyncOptions::default(), None).await.unwrap(),
            SyncResult::Updated {
                old_head: None,
                new_head: source_head.clone(),
                num_blocks: 3,
                increment: odf::dataset::MetadataChainIncrementInterval {
                    num_blocks: 3,
                    num_records: 10,
                    updated_watermark: None,
                },
            }
        );
        assert_eq!(self.destination_head().await, Some(source_head));
    }

    /// A first pull whose transfer was never committed leaves a stored
    /// dataset without `HEAD`, which the next pull continues into
    async fn assert_first_pull_continues_after_uncommitted_transfer(&self) {
        self.create_source_dataset().await;
        let source_head = self.append_source_data().await;
        self.push_source().await;

        let _abandoned = self.transfer().await;
        assert_eq!(self.destination_head().await, None);

        let transferred = self.transfer().await;
        assert_matches!(
            self.commit(transferred).await,
            Ok(SyncResult::Updated { old_head: None, new_head, .. }) if new_head == source_head
        );
        assert_eq!(self.destination_head().await, Some(source_head));
    }

    /// A retry after an uncommitted transfer takes the data and checkpoint
    /// files the destination already has instead of downloading them: with
    /// the repository's copies removed, a download would fail
    async fn assert_retry_reuses_objects_of_uncommitted_transfer(&self, repo_dir: &Path) {
        self.create_source_dataset().await;
        let source_head = self.append_source_data().await;
        self.push_source().await;

        let _abandoned = self.transfer().await;
        self.remove_repository_objects(repo_dir);

        let listener = Arc::new(StatsRecordingListener::default());
        let transferred = self.transfer_with_listener(Some(listener.clone())).await;

        // Nothing copied is counted or estimated
        let stats = listener.last_stats();
        for party in [
            &stats.src,
            &stats.src_estimated,
            &stats.dst,
            &stats.dst_estimated,
        ] {
            assert_eq!(
                (
                    party.data_slices_read + party.data_slices_written,
                    party.checkpoints_read + party.checkpoints_written,
                    party.bytes_read + party.bytes_written
                ),
                (0, 0, 0),
                "{stats:?}"
            );
        }
        assert_matches!(
            self.commit(transferred).await,
            Ok(SyncResult::Updated { new_head, .. }) if new_head == source_head
        );
    }

    /// Deletes the data and checkpoint files of the dataset in a local
    /// repository, keeping its metadata
    fn remove_repository_objects(&self, repo_dir: &Path) {
        let dataset_dir = repo_dir.join(self.dataset_alias.dataset_name.as_str());
        for objects_dir in ["data", "checkpoints"] {
            let objects_dir = dataset_dir.join(objects_dir);
            assert!(objects_dir.exists(), "{} is missing", objects_dir.display());
            std::fs::remove_dir_all(&objects_dir).unwrap();
            std::fs::create_dir(&objects_dir).unwrap();
        }
    }

    /// A first pull that copies a data or checkpoint file whose content does
    /// not match its hash fails and creates nothing
    async fn assert_pull_rejects_tampered_object(&self, objects: RepositoryObjects) {
        self.create_source_dataset().await;
        self.append_source_data().await;
        self.push_source().await;
        self.tamper_repository_object(objects);

        assert_matches!(
            self.pull(SyncOptions::default(), None).await,
            Err(SyncError::Corrupted(_))
        );
        assert_eq!(self.destination_head().await, None);
    }

    /// The dataset's directory in a local repository
    fn repository_dataset_dir(&self) -> PathBuf {
        self.repo_url
            .to_file_path()
            .unwrap()
            .join(self.dataset_alias.dataset_name.as_str())
    }

    /// Rewrites the one block `is_target` picks in a local repository with a
    /// later system time, under its old hash
    fn tamper_repository_block(
        &self,
        is_target: impl Fn(&odf::Multihash, &odf::MetadataBlock) -> bool,
    ) {
        let mut num_tampered = 0;
        for entry in std::fs::read_dir(self.repository_dataset_dir().join("blocks")).unwrap() {
            let path = entry.unwrap().path();
            let hash = odf::Multihash::from_multibase(path.file_name().unwrap().to_str().unwrap())
                .unwrap();
            let mut block =
                odf::storage::deserialize_metadata_block(&hash, &std::fs::read(&path).unwrap())
                    .unwrap();
            if !is_target(&hash, &block) {
                continue;
            }

            block.system_time += chrono::TimeDelta::seconds(1);
            std::fs::write(
                &path,
                odf::storage::serialize_metadata_block(&block)
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            num_tampered += 1;
        }
        assert_eq!(num_tampered, 1);
    }

    /// Flips a byte of the only file of `objects` in a local repository,
    /// under its old hash
    fn tamper_repository_object(&self, objects: RepositoryObjects) {
        let paths: Vec<_> =
            std::fs::read_dir(self.repository_dataset_dir().join(objects.dir_name()))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect();
        let [path] = paths.as_slice() else {
            panic!("Expected one file of {objects:?}, found {paths:?}");
        };

        let mut bytes = std::fs::read(path).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(path, bytes).unwrap();
    }

    /// Another writer moves the destination `HEAD` while a pull transfers.
    /// The commit must fail and leave the other writer's `HEAD`, with or
    /// without `force`
    async fn assert_pull_keeps_head_moved_during_transfer(&self, force: bool) {
        self.create_source_dataset().await;
        self.push_source().await;
        self.pull(SyncOptions::default(), None).await.unwrap();

        self.append_source_data().await;
        self.push_source().await;

        let listener = Arc::new(DestinationHeadMovingListener {
            catalog: self.destination_catalog.clone(),
            dataset_alias: self.dataset_alias.clone(),
            concurrent_head: Mutex::new(None),
        });

        let result = self
            .pull(
                SyncOptions {
                    force,
                    ..SyncOptions::default()
                },
                Some(listener.clone()),
            )
            .await;

        let concurrent_head = listener.concurrent_head.lock().unwrap().clone().unwrap();
        assert_matches!(
            result,
            Err(SyncError::HeadMoved(SyncHeadMovedError { heads: Some(e) }))
                if e.actual.as_ref() == Some(&concurrent_head)
        );
        assert_eq!(self.destination_head().await, Some(concurrent_head));
    }

    async fn create_source_dataset(&self) {
        self.create_source_dataset_from(
            MetadataFactory::dataset_snapshot()
                .name(self.dataset_alias.clone())
                .kind(odf::DatasetKind::Root)
                .push_event(MetadataFactory::set_data_schema().build())
                .build(),
        )
        .await;
    }

    /// A source of one `Seed` block, which no later block refers to
    async fn create_source_seed_only(&self) {
        self.create_source_dataset_from(
            MetadataFactory::dataset_snapshot()
                .name(self.dataset_alias.clone())
                .kind(odf::DatasetKind::Root)
                .build(),
        )
        .await;
    }

    async fn create_source_dataset_from(&self, snapshot: odf::DatasetSnapshot) {
        create_test_dataset_from_snapshot(
            self.source_catalog
                .get_one::<dyn DatasetRegistry>()
                .unwrap()
                .as_ref(),
            self.source_catalog
                .get_one::<odf::dataset::DatasetStorageUnitLocalFs>()
                .unwrap()
                .as_ref(),
            snapshot,
            self.source_catalog
                .get_one::<dyn DidGenerator>()
                .unwrap()
                .generate_dataset_id()
                .0,
            self.source_catalog
                .get_one::<dyn SystemTimeSource>()
                .unwrap()
                .now(),
        )
        .await
        .unwrap();
    }

    async fn append_source_data(&self) -> odf::Multihash {
        Self::append_random_data(&self.source_catalog, &self.dataset_alias).await
    }

    /// Commits a `SetInfo` to the source, a block without data or checkpoint
    async fn append_source_info(&self) -> odf::Multihash {
        self.source_catalog
            .get_one::<dyn DatasetRegistry>()
            .unwrap()
            .get_dataset_by_ref(&self.dataset_alias.as_local_ref())
            .await
            .unwrap()
            .commit_event(
                MetadataFactory::set_info()
                    .description("Metadata only")
                    .build()
                    .into(),
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap()
            .new_head
    }

    async fn append_destination_data(&self) -> odf::Multihash {
        Self::append_random_data(&self.destination_catalog, &self.dataset_alias).await
    }

    async fn append_random_data(
        catalog: &dill::Catalog,
        dataset_alias: &odf::DatasetAlias,
    ) -> odf::Multihash {
        DatasetTestHelper::append_random_data(
            catalog.get_one::<dyn DatasetRegistry>().unwrap().as_ref(),
            dataset_alias,
            32,
        )
        .await
    }

    async fn push_source(&self) {
        let request = self
            .source_catalog
            .get_one::<SyncRequestBuilder>()
            .unwrap()
            .build_sync_request(
                self.dataset_alias.as_local_ref(),
                self.remote_ref().as_any_ref(),
                true,
            )
            .await
            .unwrap();

        self.source_catalog
            .get_one::<dyn SyncService>()
            .unwrap()
            .sync(request, SyncOptions::default(), None)
            .await
            .unwrap();
    }

    async fn pull(
        &self,
        options: SyncOptions,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let request = self.build_request(self.remote_ref()).await?;
        LocalSyncHelper::new(&self.destination_catalog)
            .transfer_and_commit(request, options, maybe_listener)
            .await
    }

    /// Pulls from `remote_ref` instead of the harness's own remote
    async fn pull_from(&self, remote_ref: odf::DatasetRefRemote) -> Result<SyncResult, SyncError> {
        let request = self.build_request(remote_ref).await?;
        LocalSyncHelper::new(&self.destination_catalog)
            .transfer_and_commit(request, SyncOptions::default(), None)
            .await
    }

    async fn build_request(
        &self,
        remote_ref: odf::DatasetRefRemote,
    ) -> Result<LocalSyncRequest, SyncError> {
        LocalSyncHelper::new(&self.destination_catalog)
            .build_request(remote_ref, self.dataset_alias.as_local_ref(), true)
            .await
    }

    async fn transfer(&self) -> TransferredSync {
        self.transfer_with_listener(None).await
    }

    async fn transfer_with_listener(
        &self,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> TransferredSync {
        let request = self.build_request(self.remote_ref()).await.unwrap();

        let transfer = self
            .destination_catalog
            .get_one::<dyn SyncService>()
            .unwrap()
            .transfer_into_local(request, SyncOptions::default(), listener)
            .await
            .unwrap();
        let SyncTransfer::Transferred(transferred) = transfer else {
            panic!("Expected a transfer, got {transfer:?}");
        };
        transferred
    }

    /// Transfers into a destination dataset stored without `HEAD` from its own
    /// `Seed`, as an interrupted creation leaves it
    async fn transfer_into_headless_dataset(
        &self,
        dataset_name: &str,
    ) -> Result<SyncTransfer, SyncError> {
        let seed_block = MetadataFactory::metadata_block(
            MetadataFactory::seed(odf::DatasetKind::Root)
                .id_from(dataset_name)
                .build(),
        )
        .build_typed();
        let stored = self
            .destination_catalog
            .get_one::<dyn odf::dataset::DatasetStorageUnitWriter>()
            .unwrap()
            .store_dataset(seed_block, odf::dataset::StoreDatasetOpts::default())
            .await
            .unwrap();
        let handle = odf::DatasetHandle::new(
            stored.dataset_id,
            odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            stored.dataset_kind,
        );

        self.destination_catalog
            .get_one::<dyn SyncService>()
            .unwrap()
            .transfer_into_local(
                LocalSyncRequest {
                    src: self.remote_ref(),
                    dst: LocalSyncTarget::Existing {
                        dataset: ResolvedDataset::new(stored.dataset, handle),
                        expected_head: None,
                    },
                },
                SyncOptions::default(),
                None,
            )
            .await
    }

    async fn commit(&self, transferred: TransferredSync) -> Result<SyncResult, SyncError> {
        self.destination_catalog
            .get_one::<dyn LocalSyncCommitter>()
            .unwrap()
            .commit(transferred)
            .await
    }

    async fn delete_destination(&self) {
        let dataset_id = self
            .destination_catalog
            .get_one::<dyn DatasetRegistry>()
            .unwrap()
            .resolve_dataset_handle_by_ref(&self.dataset_alias.as_local_ref())
            .await
            .unwrap()
            .id;

        self.destination_catalog
            .get_one::<dyn odf::dataset::DatasetStorageUnitWriter>()
            .unwrap()
            .delete_dataset(&dataset_id)
            .await
            .unwrap();
    }

    /// `None` while the destination has no committed `HEAD`, read from the
    /// storage of the dataset the pull creates
    async fn destination_head(&self) -> Option<odf::Multihash> {
        let dataset = match self
            .destination_catalog
            .get_one::<odf::dataset::DatasetStorageUnitLocalFs>()
            .unwrap()
            .get_stored_dataset_by_id(&self.source_dataset_id().await)
            .await
        {
            Ok(dataset) => dataset,
            Err(odf::dataset::GetStoredDatasetError::UnresolvedId(_)) => return None,
            Err(e @ odf::dataset::GetStoredDatasetError::Internal(_)) => panic!("{e:?}"),
        };

        match dataset
            .as_metadata_chain()
            .as_uncached_ref_repo()
            .get(odf::BlockRef::Head.as_str())
            .await
        {
            Ok(head) => Some(head),
            Err(odf::GetRefError::NotFound(_)) => None,
            Err(e @ (odf::GetRefError::Access(_) | odf::GetRefError::Internal(_))) => {
                panic!("{e:?}")
            }
        }
    }

    async fn destination_alias_resolves(&self) -> bool {
        self.destination_catalog
            .get_one::<dyn DatasetRegistry>()
            .unwrap()
            .try_resolve_dataset_handle_by_ref(&self.dataset_alias.as_local_ref())
            .await
            .unwrap()
            .is_some()
    }

    /// Whether the dataset a pull created is publicly readable, as the
    /// destination's access properties record it
    async fn destination_allows_public_read(&self) -> bool {
        self.destination_catalog
            .get_one::<dyn RebacService>()
            .unwrap()
            .get_dataset_properties(&self.source_dataset_id().await)
            .await
            .unwrap()
            .allows_public_read
    }

    async fn source_dataset_id(&self) -> odf::DatasetID {
        self.source_catalog
            .get_one::<dyn DatasetRegistry>()
            .unwrap()
            .resolve_dataset_handle_by_ref(&self.dataset_alias.as_local_ref())
            .await
            .unwrap()
            .id
    }

    fn remote_ref(&self) -> odf::DatasetRefRemote {
        odf::DatasetRefRemote::from(
            &self
                .repo_url
                .join(&format!("{}/", self.dataset_alias.dataset_name))
                .unwrap(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The files a block references, in a repository directory of their own
#[derive(Debug, Clone, Copy)]
enum RepositoryObjects {
    Data,
    Checkpoints,
}

impl RepositoryObjects {
    fn dir_name(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Checkpoints => "checkpoints",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits a block to the destination the way another writer would, once
/// the pull has read its `HEAD` and before the transfer is committed
struct DestinationHeadMovingListener {
    catalog: dill::Catalog,
    dataset_alias: odf::DatasetAlias,
    concurrent_head: Mutex<Option<odf::Multihash>>,
}

/// Keeps the last stats a sync reported
#[derive(Default)]
struct StatsRecordingListener {
    last_stats: Mutex<Option<SyncStats>>,
}

impl StatsRecordingListener {
    fn last_stats(&self) -> SyncStats {
        self.last_stats.lock().unwrap().clone().unwrap()
    }
}

impl SyncListener for StatsRecordingListener {
    fn on_status(&self, _stage: SyncStage, stats: &SyncStats) {
        *self.last_stats.lock().unwrap() = Some(stats.clone());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SyncListener for DestinationHeadMovingListener {
    fn on_status(&self, stage: SyncStage, _stats: &SyncStats) {
        if stage != SyncStage::TransferData || self.concurrent_head.lock().unwrap().is_some() {
            return;
        }

        let concurrent_head = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(LocalPullHarness::append_random_data(
                &self.catalog,
                &self.dataset_alias,
            ))
        });
        *self.concurrent_head.lock().unwrap() = Some(concurrent_head);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
