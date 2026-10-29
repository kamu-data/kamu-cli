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
use std::sync::Arc;

use kamu::domain::*;
use kamu::testing::*;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::*;
use kamu_datasets::{CreateDatasetResult, ResolvedDataset};
use pretty_assertions::assert_eq;
use test_utils::HttpFileServer;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const KEY_ID: &str = "k51qzi5uqu5dhkeyid";
const KEY_NAME: &str = "test-key";
const PUBLISHED_CID: &str = "bafypublished";
const ADDED_CID: &str = "bafyadded";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_to_unknown_key_fails_without_adding() {
    let harness = IpnsPushHarness::new(IpnsPushHarness::ipfs_node(None)).await;
    let foo = harness.create_dataset("foo").await;

    let result = harness
        .push_to_key(&foo, "k51qzi5uqu5dhunknown", SyncOptions::default())
        .await;

    assert_matches!(result, Err(SyncError::InvalidDestination(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_first_push_adds_and_publishes() {
    let mut ipfs_node = IpnsPushHarness::ipfs_node(None);
    IpnsPushHarness::expect_add(&mut ipfs_node);
    IpnsPushHarness::expect_publish(&mut ipfs_node, ADDED_CID, false);
    let harness = IpnsPushHarness::new(ipfs_node).await;
    let foo = harness.create_dataset("foo").await;

    let result = harness.push(&foo, SyncOptions::default()).await.unwrap();

    assert_matches!(
        result,
        SyncResult::Updated { old_head: None, new_head, .. } if new_head == foo.head
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_first_push_without_create_fails_as_not_found() {
    let harness = IpnsPushHarness::new(IpnsPushHarness::ipfs_node(None)).await;
    let foo = harness.create_dataset("foo").await;

    let result = harness
        .push(
            &foo,
            SyncOptions {
                create_if_not_exists: false,
                ..SyncOptions::default()
            },
        )
        .await;

    assert_matches!(result, Err(SyncError::DatasetNotFound(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_add_failure_is_reported_without_publishing() {
    let mut ipfs_node = IpnsPushHarness::ipfs_node(None);
    ipfs_node.expect_add_dataset().times(1).returning(|_| {
        Err(IpfsAddError::UnsupportedIpfsStorageType(
            UnsupportedIpfsStorageTypeError {
                url: Url::parse("s3://bucket/foo/").unwrap(),
            },
        ))
    });
    let harness = IpnsPushHarness::new(ipfs_node).await;
    let foo = harness.create_dataset("foo").await;

    let result = harness.push(&foo, SyncOptions::default()).await;

    assert_matches!(result, Err(SyncError::UnsupportedIpfsStorageType(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_at_published_head_refreshes_the_name_without_adding() {
    let mut ipfs_node = IpnsPushHarness::ipfs_node(Some(PUBLISHED_CID));
    IpnsPushHarness::expect_publish(&mut ipfs_node, PUBLISHED_CID, true);
    let harness = IpnsPushHarness::new(ipfs_node).await;
    let foo = harness.create_dataset("foo").await;
    harness.publish_copy(&foo);

    let result = harness.push(&foo, SyncOptions::default()).await.unwrap();

    assert_eq!(SyncResult::UpToDate, result);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_ahead_of_published_head_adds_and_publishes() {
    let mut ipfs_node = IpnsPushHarness::ipfs_node(Some(PUBLISHED_CID));
    IpnsPushHarness::expect_add(&mut ipfs_node);
    IpnsPushHarness::expect_publish(&mut ipfs_node, ADDED_CID, false);
    let harness = IpnsPushHarness::new(ipfs_node).await;
    let foo = harness.create_dataset("foo").await;
    harness.publish_copy(&foo);
    let new_head = harness.advance(&foo, "ahead").await;

    let result = harness.push(&foo, SyncOptions::default()).await.unwrap();

    assert_matches!(
        result,
        SyncResult::Updated { old_head: Some(old_head), new_head: pushed_head, num_blocks: 1, .. }
            if old_head == foo.head && pushed_head == new_head
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_behind_published_head_fails_without_publishing() {
    let harness = IpnsPushHarness::new(IpnsPushHarness::ipfs_node(Some(PUBLISHED_CID))).await;
    let foo = harness.create_dataset("foo").await;
    harness.publish_ahead_of(&foo).await;

    let result = harness.push(&foo, SyncOptions::default()).await;

    assert_matches!(result, Err(SyncError::DestinationAhead(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_forced_push_behind_published_head_adds_and_publishes() {
    let mut ipfs_node = IpnsPushHarness::ipfs_node(Some(PUBLISHED_CID));
    IpnsPushHarness::expect_add(&mut ipfs_node);
    IpnsPushHarness::expect_publish(&mut ipfs_node, ADDED_CID, false);
    let harness = IpnsPushHarness::new(ipfs_node).await;
    let foo = harness.create_dataset("foo").await;
    let published_head = harness.publish_ahead_of(&foo).await;

    let result = harness
        .push(
            &foo,
            SyncOptions {
                force: true,
                ..SyncOptions::default()
            },
        )
        .await
        .unwrap();

    assert_matches!(
        result,
        SyncResult::Updated { old_head: Some(old_head), new_head, .. }
            if old_head == published_head && new_head == foo.head
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_push_diverged_from_published_head_fails_without_publishing() {
    let harness = IpnsPushHarness::new(IpnsPushHarness::ipfs_node(Some(PUBLISHED_CID))).await;
    let foo = harness.create_dataset("foo").await;
    harness.publish_ahead_of(&foo).await;
    harness.advance(&foo, "diverged").await;

    let result = harness.push(&foo, SyncOptions::default()).await;

    assert_matches!(result, Err(SyncError::DatasetsDiverged(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_forced_push_of_another_dataset_fails_without_publishing() {
    let harness = IpnsPushHarness::new(IpnsPushHarness::ipfs_node(Some(PUBLISHED_CID))).await;
    let foo = harness.create_dataset("foo").await;
    let bar = harness.create_dataset("bar").await;
    harness.publish_copy(&foo);

    let result = harness
        .push(
            &bar,
            SyncOptions {
                force: true,
                ..SyncOptions::default()
            },
        )
        .await;

    assert_matches!(result, Err(SyncError::DatasetIdMismatch(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pushes to `ipns://` URLs against a mocked IPFS node. The IPFS gateway is
/// an HTTP server over a local directory, so a dataset published under the
/// key is a copy of its directory at `ipns/<key>/` there. A mock without an
/// expectation for an add or a publish fails the test if the push makes one.
#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct IpnsPushHarness {
    base_repo_harness: BaseRepoHarness,
    sync_service: Arc<dyn SyncService>,
    dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
    gateway_dir: PathBuf,
}

impl IpnsPushHarness {
    async fn new(ipfs_node: MockIpfsService) -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let gateway_dir = base_repo_harness.temp_dir_path().join("gateway");
        std::fs::create_dir(&gateway_dir).unwrap();
        let gateway_server = HttpFileServer::new(&gateway_dir).await;
        let gateway_url = Url::parse(&format!("http://{}/", gateway_server.local_addr())).unwrap();
        tokio::spawn(gateway_server.run());

        let mut b = dill::CatalogBuilder::new_chained(base_repo_harness.catalog());
        b.add_value(odf::dataset::IpfsGateway {
            url: gateway_url,
            pre_resolve_dnslink: false,
        })
        .add::<odf::dataset::DatasetFactoryImpl>()
        .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
        .add::<SyncServiceImpl>()
        .add_value(ipfs_node)
        .bind::<dyn IpfsService, MockIpfsService>()
        .add::<RemoteDatasetUrlResolverImpl>()
        .add::<RemoteRepositoryRegistryImpl>()
        .add_value(RemoteReposDir::new_with_creation(
            base_repo_harness.temp_dir_path().join("repos"),
        ))
        .add::<DummySmartTransferProtocolClient>()
        .add::<SimpleTransferProtocol>();
        let catalog = b.build();

        Self {
            base_repo_harness,
            sync_service: catalog.get_one().unwrap(),
            dataset_factory: catalog.get_one().unwrap(),
            gateway_dir,
        }
    }

    /// A node that owns the key [`KEY_ID`], whose name resolves locally to
    /// `published_cid` when it is given
    fn ipfs_node(published_cid: Option<&'static str>) -> MockIpfsService {
        let mut ipfs_node = MockIpfsService::new();
        ipfs_node.expect_find_ipns_key().returning(|key_id| {
            Ok((key_id == KEY_ID).then(|| IpnsKey {
                id: KEY_ID.to_string(),
                name: KEY_NAME.to_string(),
            }))
        });
        ipfs_node
            .expect_resolve_ipns_name_locally()
            .returning(move |_| Ok(published_cid.map(str::to_string)));
        ipfs_node
    }

    fn expect_add(ipfs_node: &mut MockIpfsService) {
        ipfs_node
            .expect_add_dataset()
            .times(1)
            .returning(|_| Ok(ADDED_CID.to_string()));
    }

    fn expect_publish(ipfs_node: &mut MockIpfsService, cid: &'static str, check_resolves: bool) {
        ipfs_node
            .expect_publish_ipns_name()
            .withf(move |key, published_cid, checked| {
                key.name == KEY_NAME && published_cid == cid && *checked == check_resolves
            })
            .times(1)
            .returning(|_, _, _| Ok(()));
    }

    async fn create_dataset(&self, name: &str) -> CreateDatasetResult {
        self.create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::new_unchecked(name),
        ))
        .await
    }

    /// Appends a block and moves the dataset's `HEAD` to it
    async fn advance(&self, created: &CreateDatasetResult, description: &str) -> odf::Multihash {
        let old_head = self.get_head(created).await;
        let new_head = self.append_unreferenced_block(created, description).await;
        self.move_head(created, &old_head, &new_head).await;
        new_head
    }

    /// Publishes the dataset as it is now under [`KEY_ID`]
    fn publish_copy(&self, created: &CreateDatasetResult) {
        let dataset_dir = created
            .dataset
            .get_storage_internal_url()
            .to_file_path()
            .unwrap();
        Self::copy_dir(&dataset_dir, &self.gateway_dir.join("ipns").join(KEY_ID));
    }

    /// Publishes the dataset one block ahead of its local `HEAD`, which stays
    /// where it was; returns the published head
    async fn publish_ahead_of(&self, created: &CreateDatasetResult) -> odf::Multihash {
        let published_head = self.advance(created, "published").await;
        self.publish_copy(created);
        self.move_head(created, &published_head, &created.head)
            .await;
        published_head
    }

    async fn push(
        &self,
        created: &CreateDatasetResult,
        options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        self.push_to_key(created, KEY_ID, options).await
    }

    async fn push_to_key(
        &self,
        created: &CreateDatasetResult,
        key_id: &str,
        options: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        let url = Url::parse(&format!("ipns://{key_id}")).unwrap();
        let dataset = self.dataset_factory.get_dataset(&url, true).await.unwrap();
        let request = SyncRequest {
            src: ResolvedDataset::from_created(created),
            dst: SyncRefRemote {
                url: Arc::new(url.clone()),
                dataset,
                original_remote_ref: odf::DatasetRefRemote::Url(Arc::new(url)),
            },
        };
        self.sync_service.sync(request, options, None).await
    }

    fn copy_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).unwrap();
        for entry in std::fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let target = dst.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                Self::copy_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
