// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::{Arc, Mutex};

use kamu::domain::*;
use kamu::testing::*;
use kamu::*;
use kamu_datasets::*;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
use test_utils::LocalS3Server;
use time_source::SystemTimeSource;
use url::Url;

use crate::utils::SyncWorkspaceCatalog;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_forced_push_to_local_fs_with_remote_head_moved() {
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    RemotePushHarness::new()
        .assert_forced_push_keeps_concurrent_remote_head(repo_url)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_forced_push_to_s3_with_remote_head_moved() {
    let s3 = LocalS3Server::new().await;
    s3.set_credentials_env_vars();

    RemotePushHarness::new()
        .assert_forced_push_keeps_concurrent_remote_head(s3.url.clone())
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RemotePushHarness {
    _tmp_workspace_dir: tempfile::TempDir,
    catalog: dill::Catalog,
    dataset_alias: odf::DatasetAlias,
}

impl RemotePushHarness {
    fn new() -> Self {
        let tmp_workspace_dir = tempfile::tempdir().unwrap();
        let catalog = SyncWorkspaceCatalog::new(tmp_workspace_dir.path()).build();

        Self {
            _tmp_workspace_dir: tmp_workspace_dir,
            catalog,
            dataset_alias: odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo")),
        }
    }

    /// Pushes a dataset, advances it locally, then pushes it again with
    /// `force` while another writer moves the remote `HEAD` after the push
    /// read it. The push must fail and leave the other writer's `HEAD`
    async fn assert_forced_push_keeps_concurrent_remote_head(&self, repo_url: Url) {
        self.create_dataset().await;
        self.push(&repo_url, None).await.unwrap();
        self.append_local_data().await;

        let concurrent_head = odf::Multihash::from_digest_sha3_256(b"concurrent-writer");
        let listener = Arc::new(RemoteHeadMovingListener {
            dataset_factory: self.catalog.get_one().unwrap(),
            dataset_url: self.remote_dataset_url(&repo_url),
            concurrent_head: Mutex::new(Some(concurrent_head.clone())),
        });

        assert_matches!(
            self.push(&repo_url, Some(listener)).await,
            Err(SyncError::HeadMoved(SyncHeadMovedError { heads: Some(e) }))
                if e.actual.as_ref() == Some(&concurrent_head)
        );
        assert_eq!(self.remote_head(&repo_url).await, concurrent_head);
    }

    async fn create_dataset(&self) {
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(self.dataset_alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_data_schema().build())
            .build();

        create_test_dataset_from_snapshot(
            self.catalog
                .get_one::<dyn DatasetRegistry>()
                .unwrap()
                .as_ref(),
            self.catalog
                .get_one::<odf::dataset::DatasetStorageUnitLocalFs>()
                .unwrap()
                .as_ref(),
            snapshot,
            self.catalog
                .get_one::<dyn DidGenerator>()
                .unwrap()
                .generate_dataset_id()
                .0,
            self.catalog
                .get_one::<dyn SystemTimeSource>()
                .unwrap()
                .now(),
        )
        .await
        .unwrap();
    }

    async fn append_local_data(&self) {
        DatasetTestHelper::append_random_data(
            self.catalog
                .get_one::<dyn DatasetRegistry>()
                .unwrap()
                .as_ref(),
            &self.dataset_alias,
            32,
        )
        .await;
    }

    async fn push(
        &self,
        repo_url: &Url,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let push_ref = odf::DatasetRefRemote::from(&self.remote_dataset_url(repo_url));
        let request = self
            .catalog
            .get_one::<SyncRequestBuilder>()
            .unwrap()
            .build_sync_request(self.dataset_alias.as_local_ref(), push_ref.clone(), true)
            .await
            .unwrap();

        self.catalog
            .get_one::<dyn SyncService>()
            .unwrap()
            .sync(
                request,
                SyncOptions {
                    force: true,
                    ..SyncOptions::default()
                },
                maybe_listener,
            )
            .await
    }

    fn remote_dataset_url(&self, repo_url: &Url) -> Url {
        repo_url
            .join(&format!("{}/", self.dataset_alias.dataset_name))
            .unwrap()
    }

    async fn remote_head(&self, repo_url: &Url) -> odf::Multihash {
        self.catalog
            .get_one::<dyn odf::dataset::DatasetFactory>()
            .unwrap()
            .get_dataset(&self.remote_dataset_url(repo_url), false)
            .await
            .unwrap()
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Moves the remote `HEAD` the way another writer would, once the push has
/// read the remote head and before it commits
struct RemoteHeadMovingListener {
    dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
    dataset_url: Url,
    concurrent_head: Mutex<Option<odf::Multihash>>,
}

impl SyncListener for RemoteHeadMovingListener {
    fn on_status(&self, stage: SyncStage, _stats: &SyncStats) {
        if stage != SyncStage::TransferData {
            return;
        }
        let Some(concurrent_head) = self.concurrent_head.lock().unwrap().take() else {
            return;
        };

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.dataset_factory
                    .get_dataset(&self.dataset_url, false)
                    .await
                    .unwrap()
                    .as_metadata_chain()
                    .as_uncached_ref_repo()
                    .set(odf::BlockRef::Head.as_str(), &concurrent_head)
                    .await
                    .unwrap();
            });
        });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
