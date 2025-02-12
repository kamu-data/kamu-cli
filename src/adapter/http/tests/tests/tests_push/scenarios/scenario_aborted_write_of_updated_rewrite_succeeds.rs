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
    commit_add_data_event,
    copy_dataset_files,
    copy_folder_recursively,
    make_dataset_ref,
    write_dataset_alias,
    ClientSideHarness,
    ServerSideHarness,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPushAbortedWriteOfUpdatedWriteSucceeds<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_layout: DatasetLayout,
    pub client_dataset_layout: DatasetLayout,
    pub server_dataset_ref: odf::DatasetRefRemote,
    pub client_dataset_ref: odf::DatasetRef,
    pub client_create_result: odf::CreateDatasetResult,
    pub client_commit_result: odf::dataset::CommitResult,
}

impl<TServerHarness: ServerSideHarness>
    SmartPushAbortedWriteOfUpdatedWriteSucceeds<TServerHarness>
{
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
            client_harness.dataset_layout(&client_create_result.dataset_handle.id, "foo");

        let foo_name = odf::DatasetName::new_unchecked("foo");

        let server_dataset_layout = server_harness.dataset_layout(&odf::DatasetHandle::new(
            client_create_result.dataset_handle.id.clone(),
            odf::DatasetAlias::new(server_account_name.clone(), foo_name.clone()),
        ));

        // Hard folder synchronization
        copy_dataset_files(&client_dataset_layout, &server_dataset_layout).unwrap();

        write_dataset_alias(
            &server_dataset_layout,
            &odf::DatasetAlias::new(server_account_name.clone(), foo_name.clone()),
        )
        .await;

        server_harness
            .cli_dataset_entry_writer()
            .create_entry(
                &client_create_result.dataset_handle.id,
                &server_harness.server_account_id(),
                &client_create_result.dataset_handle.alias.dataset_name,
            )
            .await
            .unwrap();

        let client_registry = client_harness.dataset_registry();

        // Extend client-side dataset with new nodes
        let client_dataset_ref = make_dataset_ref(client_account_name.as_ref(), "foo");
        let client_dataset_handle = client_registry
            .resolve_dataset_handle_by_ref(&client_dataset_ref)
            .await
            .unwrap();
        client_harness
            .commit_dataset_event()
            .execute(
                &client_dataset_handle,
                odf::MetadataEvent::SetInfo(
                    MetadataFactory::set_info()
                        .description("updated description")
                        .build(),
                ),
            )
            .await
            .unwrap();

        let client_commit_result = commit_add_data_event(
            client_registry.as_ref(),
            &client_dataset_ref,
            &client_dataset_layout,
            None,
        )
        .await;

        // Let's pretend that previous attempts uploaded new data files, but the commit
        // did not succeed in general. To mimic this, artificially copy just the
        // data folder, containing a data block
        copy_folder_recursively(
            &client_dataset_layout.data_dir,
            &server_dataset_layout.data_dir,
        )
        .unwrap();

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
            client_create_result,
            client_commit_result,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
