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
    copy_folder_recursively,
    make_dataset_ref,
    write_dataset_alias,
    ClientSideHarness,
    ServerSideHarness,
};

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPushAbortedWriteOfNewWriteSucceeds<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_layout: DatasetLayout,
    pub client_dataset_layout: DatasetLayout,
    pub server_dataset_ref: DatasetRefRemote,
    pub client_dataset_ref: DatasetRef,
    pub client_commit_result: CommitResult,
}

impl<TServerHarness: ServerSideHarness> SmartPushAbortedWriteOfNewWriteSucceeds<TServerHarness> {
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
