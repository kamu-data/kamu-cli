// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf::dataset::DatasetLayout;
use odf::metadata::testing::MetadataFactory;

use crate::harness::{
    ClientSideHarness,
    ServerSideHarness,
    commit_add_data_event,
    copy_folder_recursively,
    make_dataset_ref,
    write_dataset_alias,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPushAbortedWriteOfNewWriteSucceeds<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_layout: DatasetLayout,
    pub client_dataset_layout: DatasetLayout,
    pub server_dataset_ref: odf::DatasetRefRemote,
    pub client_dataset_ref: odf::DatasetRef,
    pub client_commit_result: odf::dataset::CommitResult,
}

impl<TServerHarness: ServerSideHarness> SmartPushAbortedWriteOfNewWriteSucceeds<TServerHarness> {
    pub async fn prepare(
        client_harness: ClientSideHarness,
        server_harness: TServerHarness,
    ) -> Self {
        let client_account_name = client_harness.operating_account_name();
        let server_account_name = server_harness.operating_account_name();

        let client_create_result = client_harness
            .create_dataset_from_snapshot()
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(
                        client_account_name.clone(),
                        odf::DatasetName::new_unchecked("foo"),
                    ))
                    .kind(odf::DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .push_event(MetadataFactory::set_data_schema().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap();

        let client_dataset_layout =
            client_harness.dataset_layout(&client_create_result.dataset_handle.id);

        let foo_name = odf::DatasetName::new_unchecked("foo");

        let server_dataset_layout = server_harness.dataset_layout(&odf::DatasetHandle::new(
            client_create_result.dataset_handle.id.clone(),
            odf::DatasetAlias::new(server_account_name.clone(), foo_name.clone()),
            odf::DatasetKind::Root,
        ));

        let client_dataset_ref = make_dataset_ref(client_account_name.as_ref(), "foo");
        let client_commit_result = commit_add_data_event(
            client_harness.dataset_registry().as_ref(),
            &client_dataset_ref,
            &client_dataset_layout,
            None,
        )
        .await;

        // Let's pretend that previous attempts uploaded some data files, but the rest
        // was discarded. To mimic this, artificially copy just the data folder,
        // containing a data block
        copy_folder_recursively(
            &client_dataset_layout.data_dir,
            &server_dataset_layout.data_dir,
        )
        .unwrap();

        write_dataset_alias(
            &server_dataset_layout,
            &odf::DatasetAlias::new(server_account_name.clone(), foo_name.clone()),
        )
        .await;

        let server_alias = odf::DatasetAlias::new(server_account_name, foo_name);
        let server_odf_url = server_harness.dataset_url(&server_alias);
        let server_dataset_ref = odf::DatasetRefRemote::from(&server_odf_url);

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
