// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::path::Path;
use std::str::FromStr;

use dill::Component;
use kamu::domain::*;
use kamu::testing::*;
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::utils::simple_transfer_protocol::SimpleTransferProtocol;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use messaging_outbox::DummyOutboxImpl;
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use odf::metadata::testing::MetadataFactory;
use test_utils::{HttpFileServer, LocalS3Server};
use time_source::SystemTimeSourceDefault;
use url::Url;

use crate::utils::IpfsDaemon;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FILE_DATA_ARRAY_SIZE: usize = 32;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_in_sync(
    dataset_repo_lhs: &DatasetStorageUnitLocalFs,
    dataset_repo_rhs: &DatasetStorageUnitLocalFs,
    lhs: impl Into<odf::DatasetRef>,
    rhs: impl Into<odf::DatasetRef>,
) {
    let lhs_layout = dataset_repo_lhs
        .get_dataset_layout(&lhs.into())
        .await
        .unwrap();
    let rhs_layout = dataset_repo_rhs
        .get_dataset_layout(&rhs.into())
        .await
        .unwrap();
    DatasetTestHelper::assert_datasets_in_sync(&lhs_layout, &rhs_layout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn do_test_sync(
    tmp_workspace_dir_foo: &Path,
    tmp_workspace_dir_bar: &Path,
    push_ref: &odf::DatasetRefRemote,
    pull_ref: &odf::DatasetRefRemote,
    ipfs: Option<(IpfsGateway, IpfsClient)>,
) {
    // Tests sync between "foo" -> remote -> "bar"
    let dataset_alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let dataset_alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let is_ipfs = ipfs.is_none();

    let (ipfs_gateway, ipfs_client) = ipfs.unwrap_or_default();

    let datasets_dir_foo = tmp_workspace_dir_foo.join("datasets");
    let datasets_dir_bar = tmp_workspace_dir_bar.join("datasets");
    std::fs::create_dir(&datasets_dir_foo).unwrap();
    std::fs::create_dir(&datasets_dir_bar).unwrap();

    let catalog_foo = dill::CatalogBuilder::new()
        .add::<DidGeneratorDefault>()
        .add::<SystemTimeSourceDefault>()
        .add_value(ipfs_gateway.clone())
        .add_value(ipfs_client.clone())
        .add_value(CurrentAccountSubject::new_test())
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir_foo))
        .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
        .bind::<dyn DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
        .add::<DatasetRegistrySoloUnitBridge>()
        .add_value(RemoteReposDir::new(tmp_workspace_dir_foo.join("repos")))
        .add::<RemoteRepositoryRegistryImpl>()
        .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
        .add::<DatasetFactoryImpl>()
        .add::<SyncServiceImpl>()
        .add::<SyncRequestBuilder>()
        .add::<DummySmartTransferProtocolClient>()
        .add::<SimpleTransferProtocol>()
        .add::<CreateDatasetUseCaseImpl>()
        .add::<DummyOutboxImpl>()
        .build();

    let catalog_bar = dill::CatalogBuilder::new()
        .add::<DidGeneratorDefault>()
        .add::<SystemTimeSourceDefault>()
        .add_value(ipfs_gateway.clone())
        .add_value(ipfs_client.clone())
        .add_value(CurrentAccountSubject::new_test())
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir_bar))
        .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
        .bind::<dyn DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
        .add::<DatasetRegistrySoloUnitBridge>()
        .add_value(RemoteReposDir::new(tmp_workspace_dir_bar.join("repos")))
        .add::<RemoteRepositoryRegistryImpl>()
        .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
        .add::<DatasetFactoryImpl>()
        .add::<SyncServiceImpl>()
        .add::<SyncRequestBuilder>()
        .add::<DummySmartTransferProtocolClient>()
        .add::<SimpleTransferProtocol>()
        .add::<CreateDatasetUseCaseImpl>()
        .add::<DummyOutboxImpl>()
        .build();

    let sync_svc_foo = catalog_foo.get_one::<dyn SyncService>().unwrap();
    let sync_request_builder_foo = catalog_foo.get_one::<SyncRequestBuilder>().unwrap();
    let storage_unit_foo = catalog_foo.get_one::<DatasetStorageUnitLocalFs>().unwrap();
    let dataset_registry_foo = catalog_foo.get_one::<dyn DatasetRegistry>().unwrap();

    let sync_svc_bar = catalog_bar.get_one::<dyn SyncService>().unwrap();
    let sync_request_builder_bar = catalog_bar.get_one::<SyncRequestBuilder>().unwrap();
    let storage_unit_bar = catalog_bar.get_one::<DatasetStorageUnitLocalFs>().unwrap();
    let dataset_registry_bar = catalog_bar.get_one::<dyn DatasetRegistry>().unwrap();

    // Dataset does not exist locally / remotely
    assert_matches!(
        sync_request_builder_foo
            .build_sync_request(dataset_alias_foo.as_any_ref(), push_ref.as_any_ref(), true)
            .await,
        Err(SyncError::DatasetNotFound(e)) if e.dataset_ref == dataset_alias_foo.as_any_ref()
    );

    assert_matches!(
        sync_request_builder_bar
            .build_sync_request(pull_ref.as_any_ref(), dataset_alias_bar.as_any_ref(), true)
            .await,
        Err(SyncError::DatasetNotFound(e)) if e.dataset_ref == pull_ref.as_any_ref()
    );

    // Add dataset
    let snapshot = MetadataFactory::dataset_snapshot()
        .name(dataset_alias_foo.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_data_schema().build())
        .build();

    let b1 = storage_unit_foo
        .create_dataset_from_snapshot(snapshot)
        .await
        .unwrap()
        .create_dataset_result
        .head;

    // Initial sync ///////////////////////////////////////////////////////////
    assert_matches!(
        sync_request_builder_foo
            .build_sync_request(dataset_alias_foo.as_any_ref(), push_ref.as_any_ref(), false)
            .await,
        Err(SyncError::DatasetNotFound(e)) if e.dataset_ref == push_ref.as_any_ref()
    );

    let sync_result = sync_svc_foo
        .sync(
            sync_request_builder_foo
                .build_sync_request(dataset_alias_foo.as_any_ref(), push_ref.as_any_ref(), true)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(
        sync_result,
        SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 2,
            ..
        } if new_head == b1
    );

    let sync_result = sync_svc_bar
        .sync(
            sync_request_builder_bar
                .build_sync_request(pull_ref.as_any_ref(), dataset_alias_bar.as_any_ref(), true)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(
        sync_result,
        SyncResult::Updated {
            old_head: None,
            new_head,
            num_blocks: 2,
            ..
        } if new_head == b1
    );

    assert_in_sync(
        &storage_unit_foo,
        &storage_unit_bar,
        &dataset_alias_foo,
        &dataset_alias_bar,
    )
    .await;

    // Subsequent sync ////////////////////////////////////////////////////////
    let _b2 = DatasetTestHelper::append_random_data(
        dataset_registry_foo.as_ref(),
        &dataset_alias_foo,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let b3 = DatasetTestHelper::append_random_data(
        dataset_registry_foo.as_ref(),
        &dataset_alias_foo,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let sync_err = sync_svc_foo
        .sync(
            sync_request_builder_foo
                .build_sync_request(pull_ref.as_any_ref(), dataset_alias_foo.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .err()
        .unwrap();
    assert_matches!(
        sync_err,
        SyncError::DestinationAhead(DestinationAheadError {
            src_head,
            dst_head, dst_ahead_size: 2 }
        )
        if src_head == b1 && dst_head == b3
    );

    let sync_result = sync_svc_foo
        .sync(
            sync_request_builder_foo
                .build_sync_request(dataset_alias_foo.as_any_ref(), push_ref.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(
        sync_result,
        SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
            ..
        } if old_head.as_ref() == Some(&b1) && new_head == b3
    );

    let sync_result = sync_svc_bar
        .sync(
            sync_request_builder_bar
                .build_sync_request(pull_ref.as_any_ref(), dataset_alias_bar.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(
        sync_result,
        SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
            ..
        } if old_head.as_ref() == Some(&b1) && new_head == b3
    );

    assert_in_sync(
        &storage_unit_foo,
        &storage_unit_bar,
        &dataset_alias_foo,
        &dataset_alias_bar,
    )
    .await;

    // Up to date /////////////////////////////////////////////////////////////
    let sync_result = sync_svc_foo
        .sync(
            sync_request_builder_foo
                .build_sync_request(dataset_alias_foo.as_any_ref(), push_ref.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(sync_result, SyncResult::UpToDate,);

    let sync_result = sync_svc_bar
        .sync(
            sync_request_builder_bar
                .build_sync_request(pull_ref.as_any_ref(), dataset_alias_bar.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(sync_result, SyncResult::UpToDate);

    assert_in_sync(
        &storage_unit_foo,
        &storage_unit_bar,
        &dataset_alias_foo,
        &dataset_alias_bar,
    )
    .await;

    // Datasets out-of-sync on push //////////////////////////////////////////////

    // Push a new block into dataset_bar (which we were pulling into before)
    let exta_head = DatasetTestHelper::append_random_data(
        dataset_registry_bar.as_ref(),
        &dataset_alias_bar,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let sync_result = sync_svc_bar
        .sync(
            sync_request_builder_bar
                .build_sync_request(dataset_alias_bar.as_any_ref(), push_ref.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(
        sync_result,
        SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 1,
            ..
        } if old_head == Some(b3.clone()) && new_head == exta_head
    );

    // Try push from dataset_foo
    let sync_err = sync_svc_foo
        .sync(
            sync_request_builder_foo
                .build_sync_request(dataset_alias_foo.as_any_ref(), push_ref.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .err()
        .unwrap();
    assert_matches!(
        sync_err,
        SyncError::DestinationAhead(DestinationAheadError {
            src_head, dst_head, dst_ahead_size: 1
        }) if src_head == b3 && dst_head == exta_head
    );

    // Try push from dataset_1 with --force: it should abandon the diverged_head
    // block
    let sync_result = sync_svc_foo
        .sync(
            sync_request_builder_foo
                .build_sync_request(dataset_alias_foo.as_any_ref(), push_ref.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions {
                force: true,
                ..SyncOptions::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_matches!(
        sync_result,
        SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 4, // full resynchronization: seed, b1, b2, b3
            ..
        } if old_head == Some(exta_head.clone()) && new_head == b3
    );

    // Try pulling dataset_bar: should fail, destination is ahead
    let sync_err = sync_svc_bar
        .sync(
            sync_request_builder_bar
                .build_sync_request(pull_ref.as_any_ref(), dataset_alias_bar.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .err()
        .unwrap();
    assert_matches!(
        sync_err,
        SyncError::DestinationAhead(DestinationAheadError {
            src_head,
            dst_head, dst_ahead_size: 1
        }) if src_head == b3 && dst_head == exta_head
    );

    // Try pulling dataset_bar with --force: should abandon diverged_head
    let sync_result = sync_svc_bar
        .sync(
            sync_request_builder_bar
                .build_sync_request(pull_ref.as_any_ref(), dataset_alias_bar.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions {
                force: true,
                ..SyncOptions::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_matches!(
        sync_result,
        SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 4, // full resynchronization: seed, b1, b2, b3
            ..
        } if old_head == Some(exta_head.clone()) && new_head == b3
    );

    // Datasets complex divergence //////////////////////////////////////////////

    let _b4 = DatasetTestHelper::append_random_data(
        dataset_registry_foo.as_ref(),
        &dataset_alias_foo,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let b5 = DatasetTestHelper::append_random_data(
        dataset_registry_foo.as_ref(),
        &dataset_alias_foo,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let b4_alt = DatasetTestHelper::append_random_data(
        dataset_registry_bar.as_ref(),
        &dataset_alias_bar,
        FILE_DATA_ARRAY_SIZE,
    )
    .await;

    let sync_result = sync_svc_foo
        .sync(
            sync_request_builder_foo
                .build_sync_request(dataset_alias_foo.as_any_ref(), push_ref.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .unwrap();
    assert_matches!(
        sync_result,
        SyncResult::Updated {
            old_head,
            new_head,
            num_blocks: 2,
            ..
        } if old_head.as_ref() == Some(&b3) && new_head == b5
    );

    let sync_err = sync_svc_bar
        .sync(
            sync_request_builder_bar
                .build_sync_request(dataset_alias_bar.as_any_ref(), push_ref.as_any_ref(), false)
                .await
                .unwrap(),
            SyncOptions::default(),
            None,
        )
        .await
        .err()
        .unwrap();
    assert_matches!(
        sync_err,
        SyncError::DatasetsDiverged(DatasetsDivergedError {
            src_head,
            dst_head,
            detail: Some(DatasetsDivergedErrorDetail {
                uncommon_blocks_in_src,
                uncommon_blocks_in_dst
            })
        })
        if src_head == b4_alt && dst_head == b5 && uncommon_blocks_in_src ==
    1 && uncommon_blocks_in_dst == 2 );

    // Datasets corrupted transfer flow /////////////////////////////////////////
    if is_ipfs {
        let _b6 = DatasetTestHelper::append_random_data(
            dataset_registry_foo.as_ref(),
            &dataset_alias_foo,
            FILE_DATA_ARRAY_SIZE,
        )
        .await;

        let dir_files =
            std::fs::read_dir(tmp_workspace_dir_foo.join("datasets/foo/checkpoints")).unwrap();
        for file_info in dir_files {
            std::fs::remove_file(file_info.unwrap().path()).unwrap();
        }

        for _i in 0..15 {
            DatasetTestHelper::append_random_data(
                dataset_registry_foo.as_ref(),
                &dataset_alias_foo,
                FILE_DATA_ARRAY_SIZE,
            )
            .await;
        }

        let sync_err = sync_svc_foo
            .sync(
                sync_request_builder_foo
                    .build_sync_request(
                        dataset_alias_foo.as_any_ref(),
                        push_ref.as_any_ref(),
                        false,
                    )
                    .await
                    .unwrap(),
                SyncOptions::default(),
                None,
            )
            .await
            .err()
            .unwrap();
        assert_matches!(
            sync_err,
            SyncError::Corrupted(CorruptedSourceError {
                message,
                ..
            }) if message == *"Source checkpoint file is missing"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sync_to_from_local_fs() {
    let tmp_workspace_dir_foo = tempfile::tempdir().unwrap();
    let tmp_workspace_dir_bar = tempfile::tempdir().unwrap();
    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    do_test_sync(
        tmp_workspace_dir_foo.path(),
        tmp_workspace_dir_bar.path(),
        &odf::DatasetRefRemote::from(&repo_url),
        &odf::DatasetRefRemote::from(&repo_url),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_sync_to_from_s3() {
    let s3 = LocalS3Server::new().await;
    let tmp_workspace_dir_foo = tempfile::tempdir().unwrap();
    let tmp_workspace_dir_bar = tempfile::tempdir().unwrap();

    do_test_sync(
        tmp_workspace_dir_foo.path(),
        tmp_workspace_dir_bar.path(),
        &odf::DatasetRefRemote::from(&s3.url),
        &odf::DatasetRefRemote::from(&s3.url),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sync_from_http() {
    let tmp_workspace_dir_foo = tempfile::tempdir().unwrap();
    let tmp_workspace_dir_bar = tempfile::tempdir().unwrap();

    let tmp_repo_dir = tempfile::tempdir().unwrap();
    let push_repo_url = Url::from_directory_path(tmp_repo_dir.path()).unwrap();

    let server = HttpFileServer::new(tmp_repo_dir.path()).await;
    let pull_repo_url = Url::from_str(&format!("http://{}/", server.local_addr())).unwrap();

    let _server_hdl = tokio::spawn(server.run());

    do_test_sync(
        tmp_workspace_dir_foo.path(),
        tmp_workspace_dir_bar.path(),
        &odf::DatasetRefRemote::from(push_repo_url),
        &odf::DatasetRefRemote::from(pull_repo_url),
        None,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_sync_to_from_ipfs() {
    let tmp_workspace_dir_foo = tempfile::tempdir().unwrap();
    let tmp_workspace_dir_bar = tempfile::tempdir().unwrap();

    let ipfs_daemon = IpfsDaemon::new().await;
    let ipfs_client = ipfs_daemon.client();
    let key_id = ipfs_client.key_gen("test").await.unwrap();
    let ipns_url = Url::from_str(&format!("ipns://{key_id}/")).unwrap();

    do_test_sync(
        tmp_workspace_dir_foo.path(),
        tmp_workspace_dir_bar.path(),
        &odf::DatasetRefRemote::from(&ipns_url),
        &odf::DatasetRefRemote::from(&ipns_url),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
