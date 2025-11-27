// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use kamu::domain::*;
use kamu::testing::*;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::DatasetRegistry;
use kamu_datasets_services::{
    AppendDatasetMetadataBatchUseCaseImpl,
    CreateDatasetUseCaseImpl,
    DependencyGraphServiceImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use odf::metadata::testing::MetadataFactory;
use test_utils::LocalS3Server;
use time_source::{SystemTimeSource, SystemTimeSourceDefault};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Create repo/bar dataset in a repo and check it appears in searches
async fn do_test_search(tmp_workspace_dir: &Path, repo_url: Url) {
    let dataset_local_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let repo_name = odf::RepoName::new_unchecked("repo");
    let dataset_remote_alias = odf::DatasetAliasRemote::try_from("repo/bar").unwrap();
    let datasets_dir = tmp_workspace_dir.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let mut b = dill::CatalogBuilder::new();
    b.add::<DidGeneratorDefault>()
        .add::<SystemTimeSourceDefault>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
            datasets_dir,
        ))
        .add::<odf::dataset::DatasetLfsBuilderDefault>()
        .add::<DatasetRegistrySoloUnitBridge>()
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .add::<RemoteRepositoryRegistryImpl>()
        .add_value(RemoteReposDir::new_with_creation(
            tmp_workspace_dir.join("repos"),
        ))
        .add_value(IpfsGateway::default())
        .add_value(kamu::utils::ipfs_wrapper::IpfsClient::default())
        .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
        .add::<DatasetFactoryImpl>()
        .add::<SyncServiceImpl>()
        .add::<RemoteAliasesRegistryImpl>()
        .add::<RemoteAliasResolverImpl>()
        .add::<SyncRequestBuilder>()
        .add::<DummySmartTransferProtocolClient>()
        .add::<SimpleTransferProtocol>()
        .add_builder(SearchServiceRemoteImpl::builder(None))
        .add::<CreateDatasetUseCaseImpl>()
        .add::<DummyOutboxImpl>()
        .add::<AppendDatasetMetadataBatchUseCaseImpl>()
        .add::<DependencyGraphServiceImpl>();

    database_common::NoOpDatabasePlugin::init_database_components(&mut b);

    let catalog = b.build();

    let did_generator = catalog.get_one::<dyn DidGenerator>().unwrap();
    let time_source = catalog.get_one::<dyn SystemTimeSource>().unwrap();
    let remote_repo_reg = catalog.get_one::<dyn RemoteRepositoryRegistry>().unwrap();
    let dataset_registry = catalog.get_one::<dyn DatasetRegistry>().unwrap();
    let dataset_storage_unit_writer = catalog
        .get_one::<dyn odf::DatasetStorageUnitWriter>()
        .unwrap();
    let sync_svc = catalog.get_one::<dyn SyncService>().unwrap();
    let sync_request_builder = catalog.get_one::<SyncRequestBuilder>().unwrap();
    let search_svc = catalog.get_one::<dyn SearchServiceRemote>().unwrap();

    // Add repository
    remote_repo_reg
        .add_repository(&repo_name, repo_url)
        .unwrap();

    // Add and sync dataset
    let _ = create_test_dataset_from_snapshot(
        dataset_registry.as_ref(),
        dataset_storage_unit_writer.as_ref(),
        MetadataFactory::dataset_snapshot()
            .name(dataset_local_alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build(),
        did_generator.generate_dataset_id().0,
        time_source.now(),
    )
    .await
    .unwrap();

    sync_svc
        .sync(
            sync_request_builder
                .build_sync_request(
                    dataset_local_alias.as_any_ref(),
                    dataset_remote_alias.as_any_ref(),
                    true,
                )
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();

    // Search!
    assert_eq!(
        search_svc
            .search(None, SearchRemoteOpts::default())
            .await
            .unwrap(),
        SearchRemoteResult {
            datasets: vec![SearchRemoteResultDataset {
                id: None,
                alias: dataset_remote_alias.clone(),
                kind: None,
                num_blocks: None,
                num_records: None,
                estimated_size_bytes: None,
            }]
        }
    );

    assert_eq!(
        search_svc
            .search(Some("bar"), SearchRemoteOpts::default())
            .await
            .unwrap(),
        SearchRemoteResult {
            datasets: vec![SearchRemoteResultDataset {
                id: None,
                alias: dataset_remote_alias.clone(),
                kind: None,
                num_blocks: None,
                num_records: None,
                estimated_size_bytes: None,
            }]
        }
    );

    assert_eq!(
        search_svc
            .search(Some("foo"), SearchRemoteOpts::default())
            .await
            .unwrap(),
        SearchRemoteResult { datasets: vec![] }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_search_local_fs() {
    let tmp_workspace_dir = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    do_test_search(tmp_workspace_dir.path(), repo_url).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_search_s3() {
    let s3 = LocalS3Server::new().await;
    let tmp_workspace_dir = tempfile::tempdir().unwrap();

    do_test_search(tmp_workspace_dir.path(), s3.url).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
