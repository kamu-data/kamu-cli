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
    domain::{CommitOpts, DatasetExt, DatasetRepositoryExt, PullResult},
    testing::{DatasetTestHelper, MetadataFactory},
};
use opendatafabric::{
    DatasetKind, DatasetName, DatasetRef, DatasetRefAny, DatasetRefRemote, MetadataEvent,
};

use crate::harness::{
    await_client_server_flow, commit_add_data_event, copy_folder_recursively, ClientSideHarness,
    ServerSideHarness,
};

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_new_dataset<T: ServerSideHarness>(server_harness: T) {
    let server_repo = server_harness.dataset_repository();
    server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let server_dataset_layout = server_harness.dataset_layout("foo");

    let dataset_ref = DatasetRef::from_str("foo").unwrap();

    let commit_result =
        commit_add_data_event(server_repo.as_ref(), &dataset_ref, &server_dataset_layout).await;

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset_result(DatasetRefAny::from(foo_dataset_ref))
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: None,
                new_head: commit_result.new_head,
                num_blocks: 3
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_existing_up_to_date_dataset<T: ServerSideHarness>(server_harness: T) {
    let server_repo = server_harness.dataset_repository();
    server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let server_dataset_layout = server_harness.dataset_layout("foo");

    let dataset_ref = DatasetRef::from_str("foo").unwrap();

    commit_add_data_event(server_repo.as_ref(), &dataset_ref, &server_dataset_layout).await;

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");

    // Hard folder synchronization
    copy_folder_recursively(
        &server_dataset_layout.root_dir,
        &client_dataset_layout.root_dir,
    )
    .unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset_result(DatasetRefAny::from(foo_dataset_ref))
            .await;

        assert_eq!(PullResult::UpToDate {}, pull_result);

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_existing_evolved_dataset<T: ServerSideHarness>(server_harness: T) {
    let server_repo = server_harness.dataset_repository();
    let create_result = server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let server_dataset_layout = server_harness.dataset_layout("foo");

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");

    // Hard folder synchronization
    copy_folder_recursively(
        &server_dataset_layout.root_dir,
        &client_dataset_layout.root_dir,
    )
    .unwrap();

    // Extend server-side dataset with new nodes

    let dataset_ref = DatasetRef::from_str("foo").unwrap();

    server_repo
        .get_dataset(&dataset_ref)
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

    let commit_result =
        commit_add_data_event(server_repo.as_ref(), &dataset_ref, &server_dataset_layout).await;

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset_result(DatasetRefAny::from(foo_dataset_ref))
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: Some(create_result.head),
                new_head: commit_result.new_head,
                num_blocks: 2
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_existing_advanced_dataset_fails<T: ServerSideHarness>(
    server_harness: T,
) {
    let server_repo = server_harness.dataset_repository();
    server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let server_dataset_layout = server_harness.dataset_layout("foo");

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");

    // Hard folder synchronization
    copy_folder_recursively(
        &server_dataset_layout.root_dir,
        &client_dataset_layout.root_dir,
    )
    .unwrap();

    // Extend client-side dataset with new node
    let client_repo = client_harness.dataset_repository();
    client_repo
        .get_dataset(&DatasetRef::from(DatasetName::try_from("foo").unwrap()))
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
        let pull_responses = client_harness
            .pull_datasets(DatasetRefAny::from(foo_dataset_ref))
            .await;

        // TODO: try expecting better error message
        assert!(pull_responses[0].result.is_err());
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_aborted_read_of_new_reread_succeeds<T: ServerSideHarness>(
    server_harness: T,
) {
    let server_repo = server_harness.dataset_repository();
    server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let server_dataset_layout = server_harness.dataset_layout("foo");

    let dataset_ref = DatasetRef::from_str("foo").unwrap();

    let commit_result =
        commit_add_data_event(server_repo.as_ref(), &dataset_ref, &server_dataset_layout).await;

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");

    // Let's pretend that previous attempts uploaded some data files, but the rest was discarded.
    // To mimic this, artifficially copy just the data folder, contaning a data block
    copy_folder_recursively(
        &server_dataset_layout.data_dir,
        &client_dataset_layout.data_dir,
    )
    .unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset_result(DatasetRefAny::from(foo_dataset_ref))
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: None,
                new_head: commit_result.new_head,
                num_blocks: 3
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_aborted_read_of_existing_evolved_dataset_reread_succeeds<
    T: ServerSideHarness,
>(
    server_harness: T,
) {
    let server_repo = server_harness.dataset_repository();
    let create_result = server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let server_dataset_layout = server_harness.dataset_layout("foo");

    let client_harness = ClientSideHarness::new();
    let client_dataset_layout = client_harness.dataset_layout("foo");

    // Hard folder synchronization
    copy_folder_recursively(
        &server_dataset_layout.root_dir,
        &client_dataset_layout.root_dir,
    )
    .unwrap();

    // Extend server-side dataset with new nodes

    let dataset_ref = DatasetRef::from_str("foo").unwrap();

    server_repo
        .get_dataset(&dataset_ref)
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

    let commit_result =
        commit_add_data_event(server_repo.as_ref(), &dataset_ref, &server_dataset_layout).await;

    // Let's pretend that previous attempts uploaded some data files, but the rest was discarded.
    // To mimic this, artifficially copy just the data folder, contaning a data block
    copy_folder_recursively(
        &server_dataset_layout.data_dir,
        &client_dataset_layout.data_dir,
    )
    .unwrap();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset_result(DatasetRefAny::from(foo_dataset_ref))
            .await;

        assert_eq!(
            PullResult::Updated {
                old_head: Some(create_result.head),
                new_head: commit_result.new_head,
                num_blocks: 2
            },
            pull_result
        );

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////
