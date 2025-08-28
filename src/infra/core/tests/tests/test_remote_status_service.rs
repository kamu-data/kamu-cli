// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use dill::CatalogBuilder;
use kamu::testing::{BaseRepoHarness, DummySmartTransferProtocolClient};
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::{
    RemoteAliasResolverImpl,
    RemoteAliasesRegistryImpl,
    RemoteReposDir,
    RemoteRepositoryRegistryImpl,
    RemoteStatusServiceImpl,
    SyncRequestBuilder,
    SyncServiceImpl,
};
use kamu_core::utils::metadata_chain_comparator::CompareChainsResult;
use kamu_core::*;
use kamu_datasets::CreateDatasetResult;
use kamu_datasets_services::{AppendDatasetMetadataBatchUseCaseImpl, DependencyGraphServiceImpl};
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use odf::metadata::testing::MetadataFactory;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_check_remotes_status_equal() {
    let harness = RemoteStatusTestHarness::new();

    let local_ds = harness.create_dataset().await;

    let remote = harness.push_dataset(&local_ds.dataset_handle).await;

    let result = harness
        .remote_status_service
        .check_remotes_status(&local_ds.dataset_handle)
        .await
        .unwrap();

    assert_eq!(result.statuses.len(), 1);
    let status = result.statuses.first().unwrap();

    assert_eq!(&status.remote, &remote);
    assert_matches!(status.check_result, Ok(CompareChainsResult::Equal));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_check_remotes_status_remote_behind() {
    let harness = RemoteStatusTestHarness::new();

    let local_ds = harness.create_dataset().await;

    let remote = harness.push_dataset(&local_ds.dataset_handle).await;

    let head = &local_ds.head;
    let schema_block = RemoteStatusTestHarness::schema_block(head);
    let local_chain = local_ds.dataset.as_metadata_chain();
    let _ = local_chain
        .append(schema_block, odf::dataset::AppendOpts::default())
        .await
        .unwrap();

    let result = harness
        .remote_status_service
        .check_remotes_status(&local_ds.dataset_handle)
        .await
        .unwrap();

    assert_eq!(result.statuses.len(), 1);
    let status = result.statuses.first().unwrap();

    assert_eq!(&status.remote, &remote);
    assert_matches!(
        status.check_result,
        Ok(CompareChainsResult::LhsAhead { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_check_remotes_status_remote_ahead() {
    let harness = RemoteStatusTestHarness::new();

    let local_ds = harness.create_dataset().await;

    let head = &local_ds.head;
    let schema_block = RemoteStatusTestHarness::schema_block(head);
    let local_chain = local_ds.dataset.as_metadata_chain();
    let _ = local_chain
        .append(schema_block, odf::dataset::AppendOpts::default())
        .await
        .unwrap();

    let remote = harness.push_dataset(&local_ds.dataset_handle).await;

    local_chain
        .set_ref(
            &odf::BlockRef::Head,
            head,
            odf::dataset::SetRefOpts {
                validate_block_present: true,
                check_ref_is: None,
            },
        )
        .await
        .unwrap();

    let result = harness
        .remote_status_service
        .check_remotes_status(&local_ds.dataset_handle)
        .await
        .unwrap();

    assert_eq!(result.statuses.len(), 1);
    let status = result.statuses.first().unwrap();

    assert_eq!(&status.remote, &remote);
    assert_matches!(
        status.check_result,
        Ok(CompareChainsResult::LhsBehind { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_check_remotes_status_remote_diverge() {
    let harness = RemoteStatusTestHarness::new();

    let local_ds = harness.create_dataset().await;

    let head = &local_ds.head;
    let schema_block = RemoteStatusTestHarness::schema_block(head);
    let local_chain = local_ds.dataset.as_metadata_chain();
    let _ = local_chain
        .append(schema_block, odf::dataset::AppendOpts::default())
        .await
        .unwrap();

    let remote = harness.push_dataset(&local_ds.dataset_handle).await;

    local_chain
        .set_ref(
            &odf::BlockRef::Head,
            head,
            odf::dataset::SetRefOpts {
                validate_block_present: true,
                check_ref_is: None,
            },
        )
        .await
        .unwrap();

    let diverge_schema_block = MetadataFactory::metadata_block(
        MetadataFactory::set_data_schema()
            .schema_from_arrow(&Schema::new(vec![Field::new(
                "city",
                DataType::Utf8,
                false,
            )]))
            .build(),
    )
    .prev(head, 1)
    .build();

    let _ = local_chain
        .append(diverge_schema_block, odf::dataset::AppendOpts::default())
        .await
        .unwrap();

    let result = harness
        .remote_status_service
        .check_remotes_status(&local_ds.dataset_handle)
        .await
        .unwrap();

    assert_eq!(result.statuses.len(), 1);
    let status = result.statuses.first().unwrap();

    assert_eq!(&status.remote, &remote);
    assert_matches!(
        status.check_result,
        Ok(CompareChainsResult::Divergence { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_check_remotes_status_not_found() {
    let harness = RemoteStatusTestHarness::new();

    let local_ds = harness.create_dataset().await;

    let remote = harness.push_dataset(&local_ds.dataset_handle).await;
    fs::remove_dir_all(harness.remote_repos_dir.join("repo1")).unwrap();

    let result = harness
        .remote_status_service
        .check_remotes_status(&local_ds.dataset_handle)
        .await
        .unwrap();

    assert_eq!(result.statuses.len(), 1);
    let status = result.statuses.first().unwrap();

    assert_eq!(&status.remote, &remote);
    assert_matches!(
        status.check_result,
        Err(StatusCheckError::RemoteDatasetNotFound)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct RemoteStatusTestHarness {
    base_repo_harness: BaseRepoHarness,
    remote_status_service: Arc<dyn RemoteStatusService>,
    sync_service: Arc<dyn SyncService>,
    sync_builder: Arc<SyncRequestBuilder>,
    remote_aliases_reg: Arc<dyn RemoteAliasesRegistry>,
    remote_repos_dir: PathBuf,
}

impl RemoteStatusTestHarness {
    fn new() -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();
        let remote_repos_dir = base_repo_harness.temp_dir_path().join("remote_repos");

        let catalog = CatalogBuilder::new_chained(base_repo_harness.catalog())
            .add::<RemoteStatusServiceImpl>()
            .add::<SyncServiceImpl>()
            .add::<SyncRequestBuilder>()
            .add::<DatasetFactoryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add_value(IpfsGateway::default())
            .add_value(IpfsClient::default())
            .add_value(RemoteReposDir::new(remote_repos_dir.clone()))
            .add::<RemoteRepositoryRegistryImpl>()
            .add::<DummySmartTransferProtocolClient>()
            .add::<SimpleTransferProtocol>()
            .add::<AppendDatasetMetadataBatchUseCaseImpl>()
            .add::<DependencyGraphServiceImpl>()
            .build();

        Self {
            base_repo_harness,
            remote_status_service: catalog.get_one().unwrap(),
            sync_service: catalog.get_one().unwrap(),
            sync_builder: catalog.get_one().unwrap(),
            remote_aliases_reg: catalog.get_one().unwrap(),
            remote_repos_dir,
        }
    }

    async fn create_dataset(&self) -> CreateDatasetResult {
        self.create_root_dataset(&odf::DatasetAlias::new(
            None,
            odf::DatasetName::new_unchecked("local"),
        ))
        .await
    }

    async fn push_dataset(&self, handle: &odf::DatasetHandle) -> odf::DatasetRefRemote {
        let repo_url = Url::from_directory_path(self.remote_repos_dir.join("repo1")).unwrap();
        let remote = odf::DatasetRefRemote::from(&repo_url);
        let sync_request = self
            .sync_builder
            .build_sync_request(handle.alias.as_any_ref(), remote.as_any_ref(), true)
            .await
            .unwrap();

        self.sync_service
            .sync(
                sync_request,
                SyncOptions {
                    trust_source: None,
                    create_if_not_exists: false,
                    force: false,
                    dataset_visibility: odf::DatasetVisibility::Public,
                },
                None,
            )
            .await
            .unwrap();

        self.remote_aliases_reg
            .get_remote_aliases(handle)
            .await
            .unwrap()
            .add(&remote, RemoteAliasKind::Push)
            .await
            .unwrap();

        remote
    }

    fn schema_block(root_hash: &odf::Multihash) -> odf::MetadataBlock {
        MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
            .prev(root_hash, 1)
            .build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
