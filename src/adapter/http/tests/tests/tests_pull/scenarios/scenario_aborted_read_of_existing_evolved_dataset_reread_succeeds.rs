// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::testing::MetadataFactory;
use kamu::DatasetLayout;
use opendatafabric::*;

use crate::harness::{
    commit_add_data_event,
    copy_dataset_files,
    copy_folder_recursively,
    make_dataset_ref,
    write_dataset_alias,
    ClientSideHarness,
    ServerSideHarness,
};

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPullAbortedReadOfExistingEvolvedRereadSucceedsScenario<
    TServerHarness: ServerSideHarness,
> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_layout: DatasetLayout,
    pub client_dataset_layout: DatasetLayout,
    pub server_dataset_ref: DatasetRefRemote,
    pub server_create_result: CreateDatasetResult,
    pub server_commit_result: CommitResult,
}

impl<TServerHarness: ServerSideHarness>
    SmartPullAbortedReadOfExistingEvolvedRereadSucceedsScenario<TServerHarness>
{
    pub async fn prepare(
        client_harness: ClientSideHarness,
        server_harness: TServerHarness,
    ) -> Self {
        let server_account_name = server_harness.operating_account_name();

        let server_repo = server_harness.cli_dataset_repository();
        let server_create_result = server_repo
            .create_dataset_from_snapshot(
                server_account_name.clone(),
                MetadataFactory::dataset_snapshot()
                    .name("foo")
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap();

        let server_dataset_layout =
            server_harness.dataset_layout(&server_create_result.dataset_handle);

        let client_dataset_layout =
            client_harness.dataset_layout(&server_create_result.dataset_handle.id, "foo");

        // Hard folder synchronization
        copy_dataset_files(&server_dataset_layout, &client_dataset_layout).unwrap();

        let foo_name = DatasetName::new_unchecked("foo");
        write_dataset_alias(
            &client_dataset_layout,
            &DatasetAlias::new(client_harness.operating_account_name(), foo_name.clone()),
        )
        .await;

        // Extend server-side dataset with new nodes

        let server_dataset_ref = make_dataset_ref(&server_account_name, "foo");

        server_repo
            .get_dataset(&server_dataset_ref)
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

        let server_commit_result = commit_add_data_event(
            server_repo.as_ref(),
            &server_dataset_ref,
            &server_dataset_layout,
        )
        .await;

        // Let's pretend that previous attempts uploaded some data files, but the rest
        // was discarded. To mimic this, artifficially copy just the data folder,
        // contaning a data block
        copy_folder_recursively(
            &server_dataset_layout.data_dir,
            &client_dataset_layout.data_dir,
        )
        .unwrap();

        let server_alias = DatasetAlias::new(server_account_name, foo_name);
        let server_odf_url = server_harness.dataset_url(&server_alias);
        let server_dataset_ref = DatasetRefRemote::from(&server_odf_url);

        Self {
            client_harness,
            server_harness,
            server_dataset_layout,
            client_dataset_layout,
            server_dataset_ref,
            server_create_result,
            server_commit_result,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
