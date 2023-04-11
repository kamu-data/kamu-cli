// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use kamu::{
    domain::{CommitOpts, DatasetExt, DatasetRepositoryExt, SyncResult},
    testing::{DatasetTestHelper, MetadataFactory},
};
use opendatafabric::{DatasetKind, DatasetRefLocal, DatasetRefRemote, MetadataEvent};

use crate::harness::{
    await_client_server_flow, copy_folder_recursively, create_random_data, ClientSideHarness,
    ServerSideHarness,
};

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_new_dataset<T: ServerSideHarness>(server_harness: T) {
    let server_dataset_layout = server_harness.dataset_layout("foo");

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");
    let client_repo = client_harness.dataset_repository();

    client_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let local_dataset_ref = DatasetRef::from_str("foo").unwrap();

    let commit_result = client_repo
        .get_dataset(&local_dataset_ref)
        .await
        .unwrap()
        .commit_event(
            MetadataEvent::AddData(create_random_data(&client_dataset_layout).await.build()),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let push_result = client_harness
            .push_dataset_result(local_dataset_ref, foo_dataset_ref)
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: None,
                new_head: commit_result.new_head,
                num_blocks: 3
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_existing_up_to_date_dataset<T: ServerSideHarness>(server_harness: T) {
    let server_dataset_layout = server_harness.dataset_layout("foo");

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");
    let client_repo = client_harness.dataset_repository();

    client_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    // Hard folder synchronization
    copy_folder_recursively(
        &client_dataset_layout.root_dir,
        &server_dataset_layout.root_dir,
    )
    .unwrap();

    let local_dataset_ref = DatasetRef::from_str("foo").unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let push_result = client_harness
            .push_dataset_result(local_dataset_ref, foo_dataset_ref)
            .await;

        assert_eq!(SyncResult::UpToDate {}, push_result);

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_existing_evolved_dataset<T: ServerSideHarness>(server_harness: T) {
    let server_dataset_layout = server_harness.dataset_layout("foo");

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");
    let client_repo = client_harness.dataset_repository();

    let create_result = client_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    // Hard folder synchronization
    copy_folder_recursively(
        &client_dataset_layout.root_dir,
        &server_dataset_layout.root_dir,
    )
    .unwrap();

    // Extend client-side dataset with new node
    let local_dataset_ref = DatasetRef::from_str("foo").unwrap();
    let commit_result = client_repo
        .get_dataset(&local_dataset_ref)
        .await
        .unwrap()
        .commit_event(
            MetadataEvent::SetInfo(
                MetadataFactory::set_info()
                    .description("updated description")
                    .build(),
            ),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let push_result = client_harness
            .push_dataset_result(local_dataset_ref, foo_dataset_ref)
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: Some(create_result.head),
                new_head: commit_result.new_head,
                num_blocks: 1
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_existing_dataset_fails_as_server_advanced<T: ServerSideHarness>(
    server_harness: T,
) {
    let server_dataset_layout = server_harness.dataset_layout("foo");
    let server_repo = server_harness.dataset_repository();

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");
    let client_repo = client_harness.dataset_repository();

    client_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    // Hard folder synchronization
    copy_folder_recursively(
        &client_dataset_layout.root_dir,
        &server_dataset_layout.root_dir,
    )
    .unwrap();

    let local_dataset_ref = DatasetRef::from_str("foo").unwrap();

    // Extend server-side dataset with new node
    server_repo
        .get_dataset(&local_dataset_ref)
        .await
        .unwrap()
        .commit_event(
            MetadataEvent::SetInfo(
                MetadataFactory::set_info()
                    .description("updated description")
                    .build(),
            ),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let push_responses = client_harness
            .push_dataset(local_dataset_ref, foo_dataset_ref)
            .await;

        // TODO: try expecting better error message
        assert!(push_responses[0].result.is_err());
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////
