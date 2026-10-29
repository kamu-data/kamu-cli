// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::path::Path;
use std::sync::{Arc, Mutex};

use kamu::domain::*;
use kamu::testing::*;
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::*;
use kamu_datasets_inmem::*;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::*;
use odf::dataset::DatasetStorageUnit as _;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
use test_utils::LocalS3Server;
use time_source::{SystemTimeSource, SystemTimeSourceDefault};
use url::Url;

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
    // Nothing listens on the TCP port multiplexer port of the loopback
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
        Self::build(repo_url, false)
    }

    /// Another dataset takes the destination's name when a first pull creates
    /// it
    fn with_destination_alias_taken(repo_url: Url) -> Self {
        Self::build(repo_url, true)
    }

    fn build(repo_url: Url, destination_alias_taken: bool) -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();

        Self {
            source_catalog: Self::build_workspace_catalog(&tmp_dir.path().join("source"), false),
            destination_catalog: Self::build_workspace_catalog(
                &tmp_dir.path().join("destination"),
                destination_alias_taken,
            ),
            _tmp_dir: tmp_dir,
            repo_url,
            dataset_alias: odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo")),
        }
    }

    fn build_workspace_catalog(workspace_dir: &Path, alias_taken: bool) -> dill::Catalog {
        let datasets_dir = workspace_dir.join("datasets");
        std::fs::create_dir_all(&datasets_dir).unwrap();

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

        let mut b = dill::CatalogBuilder::new();
        b.add::<DidGeneratorDefault>()
            .add::<SystemTimeSourceDefault>()
            .add_value(odf::dataset::IpfsGateway::default())
            .add_value(IpfsClient::default())
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add::<DatasetRegistrySoloUnitBridge>()
            .add_value(RemoteReposDir::new_with_creation(
                workspace_dir.join("repos"),
            ))
            .add::<RemoteRepositoryRegistryImpl>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<odf::dataset::DatasetFactoryImpl>()
            .add::<SyncServiceImpl>()
            .add::<IpfsServiceImpl>()
            .add::<LocalSyncCommitterImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<RemoteDatasetUrlResolverImpl>()
            .add::<SyncRequestBuilder>()
            .add::<DummySmartTransferProtocolClient>()
            .add::<SimpleTransferProtocol>()
            .add::<CreateDatasetUseCaseImpl>()
            .add::<CreateDatasetUseCaseHelper>()
            .add_builder(OutboxImmediateImpl::builder(ConsumerFilter::AllConsumers))
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add_value(dataset_entry_writer)
            .bind::<dyn DatasetEntryWriter, MockDatasetEntryWriter>()
            .add::<DatasetReferenceServiceImpl>()
            .add::<InMemoryDatasetReferenceRepository>()
            .add::<DatasetHeadCommitterImpl>();

        register_message_dispatcher::<DatasetLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );
        register_message_dispatcher::<DatasetReferenceMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );
        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        b.build()
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
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(self.dataset_alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_data_schema().build())
            .build();

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
                self.dataset_alias.as_any_ref(),
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
        let request = self.build_request(self.remote_ref()).await.unwrap();

        let transfer = self
            .destination_catalog
            .get_one::<dyn SyncService>()
            .unwrap()
            .transfer_into_local(request, SyncOptions::default(), None)
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

/// Commits a block to the destination the way another writer would, once
/// the pull has read its `HEAD` and before the transfer is committed
struct DestinationHeadMovingListener {
    catalog: dill::Catalog,
    dataset_alias: odf::DatasetAlias,
    concurrent_head: Mutex<Option<odf::Multihash>>,
}

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
