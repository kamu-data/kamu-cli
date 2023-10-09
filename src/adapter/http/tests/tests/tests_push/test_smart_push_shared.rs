// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::testing::{DatasetTestHelper, MetadataFactory};
use kamu::DatasetLayout;
use opendatafabric::*;

use crate::harness::{
    await_client_server_flow,
    commit_add_data_event,
    copy_folder_recursively,
    make_dataset_ref,
    write_dataset_alias,
    ClientSideHarness,
    ServerSideHarness,
};

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPushNewDatasetScenario<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_layout: DatasetLayout,
    pub client_dataset_layout: DatasetLayout,
    pub server_dataset_ref: DatasetRefRemote,
    pub client_dataset_ref: DatasetRef,
    pub client_commit_result: CommitResult,
}

impl<TServerHarness: ServerSideHarness> SmartPushNewDatasetScenario<TServerHarness> {
    pub async fn prepare(
        client_harness: ClientSideHarness,
        server_harness: TServerHarness,
    ) -> Self {
        let client_account_name = client_harness.operating_account_name();
        let server_acount_name = server_harness.operating_account_name();

        let client_repo = client_harness.dataset_repository();

        let client_create_result = client_repo
            .create_dataset_from_snapshot(
                client_account_name.clone(),
                MetadataFactory::dataset_snapshot()
                    .name("foo")
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap();

        let client_dataset_layout =
            client_harness.dataset_layout(&client_create_result.dataset_handle.id, "foo");

        let foo_name = DatasetName::new_unchecked("foo");

        let server_dataset_layout = server_harness.dataset_layout(&DatasetHandle::new(
            client_create_result.dataset_handle.id.clone(),
            DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
        ));

        let client_dataset_ref = make_dataset_ref(&client_account_name, "foo");
        let client_commit_result = commit_add_data_event(
            client_repo.as_ref(),
            &client_dataset_ref,
            &client_dataset_layout,
        )
        .await;

        let server_alias = DatasetAlias::new(server_acount_name, foo_name);
        let server_odf_url = server_harness.dataset_url(&server_alias);
        let server_dataset_ref = DatasetRefRemote::from(&server_odf_url);

        Self {
            client_harness,
            server_harness,
            server_dataset_layout,
            client_dataset_layout,
            server_dataset_ref,
            client_dataset_ref,
            client_commit_result,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_new_dataset<T: ServerSideHarness>(
    a_client_harness: ClientSideHarness,
    a_server_harness: T,
) {
    let scenario = SmartPushNewDatasetScenario::prepare(a_client_harness, a_server_harness).await;

    let api_server_handle = scenario.server_harness.api_server_run();

    let client_handle = async {
        let push_result = scenario
            .client_harness
            .push_dataset_result(scenario.client_dataset_ref, scenario.server_dataset_ref)
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: None,
                new_head: scenario.client_commit_result.new_head,
                num_blocks: 3
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(
            &scenario.server_dataset_layout,
            &scenario.client_dataset_layout,
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_existing_up_to_date_dataset<T: ServerSideHarness>(
    client_harness: ClientSideHarness,
    server_harness: T,
) {
    let client_account_name = client_harness.operating_account_name();
    let server_acount_name = server_harness.operating_account_name();

    let client_repo = client_harness.dataset_repository();

    let client_create_result = client_repo
        .create_dataset_from_snapshot(
            client_account_name.clone(),
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_dataset_layout =
        client_harness.dataset_layout(&client_create_result.dataset_handle.id, "foo");

    let foo_name = DatasetName::new_unchecked("foo");

    let server_dataset_layout = server_harness.dataset_layout(&DatasetHandle::new(
        client_create_result.dataset_handle.id.clone(),
        DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    ));

    let client_dataset_ref: DatasetRef = make_dataset_ref(&client_account_name, "foo");
    commit_add_data_event(
        client_repo.as_ref(),
        &client_dataset_ref,
        &client_dataset_layout,
    )
    .await;

    // Hard folder synchronization
    copy_folder_recursively(
        &client_dataset_layout.root_dir,
        &server_dataset_layout.root_dir,
    )
    .unwrap();

    write_dataset_alias(
        &server_dataset_layout,
        &DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    )
    .await;

    let server_alias = DatasetAlias::new(server_acount_name, foo_name);
    let server_odf_url = server_harness.dataset_url(&server_alias);
    let server_dataset_ref = DatasetRefRemote::from(&server_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let push_result = client_harness
            .push_dataset_result(client_dataset_ref, server_dataset_ref)
            .await;

        assert_eq!(SyncResult::UpToDate {}, push_result);

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_existing_evolved_dataset<T: ServerSideHarness>(
    client_harness: ClientSideHarness,
    server_harness: T,
) {
    let client_account_name = client_harness.operating_account_name();
    let server_acount_name = server_harness.operating_account_name();

    let client_repo = client_harness.dataset_repository();

    let client_create_result = client_repo
        .create_dataset_from_snapshot(
            client_account_name.clone(),
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_dataset_layout =
        client_harness.dataset_layout(&client_create_result.dataset_handle.id, "foo");

    let foo_name = DatasetName::new_unchecked("foo");

    let server_dataset_layout = server_harness.dataset_layout(&DatasetHandle::new(
        client_create_result.dataset_handle.id.clone(),
        DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    ));

    // Hard folder synchronization
    copy_folder_recursively(
        &client_dataset_layout.root_dir,
        &server_dataset_layout.root_dir,
    )
    .unwrap();

    write_dataset_alias(
        &server_dataset_layout,
        &DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    )
    .await;

    // Extend client-side dataset with new nodes
    let client_dataset_ref = make_dataset_ref(&client_account_name, "foo");
    client_repo
        .get_dataset(&client_dataset_ref)
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

    let commit_result = commit_add_data_event(
        client_repo.as_ref(),
        &client_dataset_ref,
        &client_dataset_layout,
    )
    .await;

    let server_alias = DatasetAlias::new(server_acount_name, foo_name);
    let server_odf_url = server_harness.dataset_url(&server_alias);
    let server_dataset_ref = DatasetRefRemote::from(&server_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let push_result = client_harness
            .push_dataset_result(client_dataset_ref, server_dataset_ref)
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: Some(client_create_result.head),
                new_head: commit_result.new_head,
                num_blocks: 2
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_existing_dataset_fails_as_server_advanced<T: ServerSideHarness>(
    client_harness: ClientSideHarness,
    server_harness: T,
) {
    let client_account_name = client_harness.operating_account_name();
    let server_acount_name = server_harness.operating_account_name();

    let server_repo = server_harness.cli_dataset_repository();
    let client_repo = client_harness.dataset_repository();

    let client_create_result = client_repo
        .create_dataset_from_snapshot(
            client_account_name.clone(),
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_dataset_layout =
        client_harness.dataset_layout(&client_create_result.dataset_handle.id, "foo");

    let foo_name = DatasetName::new_unchecked("foo");

    let server_dataset_layout = server_harness.dataset_layout(&DatasetHandle::new(
        client_create_result.dataset_handle.id.clone(),
        DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    ));

    // Hard folder synchronization
    copy_folder_recursively(
        &client_dataset_layout.root_dir,
        &server_dataset_layout.root_dir,
    )
    .unwrap();

    write_dataset_alias(
        &server_dataset_layout,
        &DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    )
    .await;

    // Extend server-side dataset with new node
    server_repo
        .get_dataset(&make_dataset_ref(&server_acount_name, "foo"))
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

    let server_alias = DatasetAlias::new(server_acount_name, foo_name);
    let server_odf_url = server_harness.dataset_url(&server_alias);
    let server_dataset_ref = DatasetRefRemote::from(&server_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let push_responses = client_harness
            .push_dataset(
                make_dataset_ref(&client_account_name, "foo"),
                server_dataset_ref,
            )
            .await;

        // TODO: try expecting better error message
        assert!(push_responses[0].result.is_err());
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_aborted_write_of_new_rewrite_succeeds<T: ServerSideHarness>(
    client_harness: ClientSideHarness,
    server_harness: T,
) {
    let client_account_name = client_harness.operating_account_name();
    let server_acount_name = server_harness.operating_account_name();

    let client_repo = client_harness.dataset_repository();

    let client_create_result = client_repo
        .create_dataset_from_snapshot(
            client_account_name.clone(),
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_dataset_layout =
        client_harness.dataset_layout(&client_create_result.dataset_handle.id, "foo");

    let foo_name = DatasetName::new_unchecked("foo");

    let server_dataset_layout = server_harness.dataset_layout(&DatasetHandle::new(
        client_create_result.dataset_handle.id.clone(),
        DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    ));

    let client_dataset_ref = make_dataset_ref(&client_account_name, "foo");
    let commit_result = commit_add_data_event(
        client_repo.as_ref(),
        &client_dataset_ref,
        &client_dataset_layout,
    )
    .await;

    // Let's pretend that previous attempts uploaded some data files, but the rest
    // was discarded. To mimic this, artifficially copy just the data folder,
    // contaning a data block
    copy_folder_recursively(
        &client_dataset_layout.data_dir,
        &server_dataset_layout.data_dir,
    )
    .unwrap();

    write_dataset_alias(
        &server_dataset_layout,
        &DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    )
    .await;

    let server_alias = DatasetAlias::new(server_acount_name, foo_name);
    let server_odf_url = server_harness.dataset_url(&server_alias);
    let server_dataset_ref = DatasetRefRemote::from(&server_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let push_result = client_harness
            .push_dataset_result(client_dataset_ref, server_dataset_ref)
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

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_aborted_write_of_updated_rewrite_succeeds<T: ServerSideHarness>(
    client_harness: ClientSideHarness,
    server_harness: T,
) {
    let client_account_name = client_harness.operating_account_name();
    let server_acount_name = server_harness.operating_account_name();

    let client_repo = client_harness.dataset_repository();

    let client_create_result = client_repo
        .create_dataset_from_snapshot(
            client_account_name.clone(),
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_dataset_layout =
        client_harness.dataset_layout(&client_create_result.dataset_handle.id, "foo");

    let foo_name = DatasetName::new_unchecked("foo");

    let server_dataset_layout = server_harness.dataset_layout(&DatasetHandle::new(
        client_create_result.dataset_handle.id.clone(),
        DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    ));

    // Hard folder synchronization
    copy_folder_recursively(
        &client_dataset_layout.root_dir,
        &server_dataset_layout.root_dir,
    )
    .unwrap();

    write_dataset_alias(
        &server_dataset_layout,
        &DatasetAlias::new(server_acount_name.clone(), foo_name.clone()),
    )
    .await;

    // Extend client-side dataset with new nodes
    let client_dataset_ref = make_dataset_ref(&&client_account_name, "foo");
    client_repo
        .get_dataset(&client_dataset_ref)
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

    let commit_result = commit_add_data_event(
        client_repo.as_ref(),
        &client_dataset_ref,
        &client_dataset_layout,
    )
    .await;

    // Let's pretend that previous attempts uploaded new data files, but the commit
    // did not succceed in general. To mimic this, artifficially copy just the
    // data folder, contaning a data block
    copy_folder_recursively(
        &client_dataset_layout.data_dir,
        &server_dataset_layout.data_dir,
    )
    .unwrap();

    let server_alias = DatasetAlias::new(server_acount_name, foo_name);
    let server_odf_url = server_harness.dataset_url(&server_alias);
    let server_dataset_ref = DatasetRefRemote::from(&server_odf_url);

    let api_server_handle = server_harness.api_server_run();
    let client_handle = async {
        let push_result = client_harness
            .push_dataset_result(client_dataset_ref, server_dataset_ref)
            .await;

        assert_eq!(
            SyncResult::Updated {
                old_head: Some(client_create_result.head),
                new_head: commit_result.new_head,
                num_blocks: 2
            },
            push_result
        );

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, &client_dataset_layout);
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////
