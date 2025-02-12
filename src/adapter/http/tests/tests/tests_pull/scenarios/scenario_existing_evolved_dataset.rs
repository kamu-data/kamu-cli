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
    make_dataset_ref,
    write_dataset_alias,
    ClientSideHarness,
    ServerSideHarness,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPullExistingEvolvedDatasetScenario<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_layout: DatasetLayout,
    pub client_dataset_layout: DatasetLayout,
    pub server_dataset_ref: odf::DatasetRefRemote,
    pub server_create_result: odf::CreateDatasetResult,
    pub server_commit_result: odf::dataset::CommitResult,
}

impl<TServerHarness: ServerSideHarness> SmartPullExistingEvolvedDatasetScenario<TServerHarness> {
    pub async fn prepare(
        client_harness: ClientSideHarness,
        server_harness: TServerHarness,
    ) -> Self {
        let server_account_name = server_harness.operating_account_name();

        let create_dataset_from_snapshot =
            server_harness.cli_create_dataset_from_snapshot_use_case();

        let server_create_result = create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(
                        server_account_name.clone(),
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

        let server_dataset_layout =
            server_harness.dataset_layout(&server_create_result.dataset_handle);

        let client_dataset_layout =
            client_harness.dataset_layout(&server_create_result.dataset_handle.id, "foo");

        // Hard folder synchronization
        copy_dataset_files(&server_dataset_layout, &client_dataset_layout).unwrap();

        let foo_name = odf::DatasetName::new_unchecked("foo");
        write_dataset_alias(
            &client_dataset_layout,
            &odf::DatasetAlias::new(client_harness.operating_account_name(), foo_name.clone()),
        )
        .await;

        client_harness
            .dataset_entry_writer()
            .create_entry(
                &server_create_result.dataset_handle.id,
                &client_harness.client_account_id(),
                &server_create_result.dataset_handle.alias.dataset_name,
            )
            .await
            .unwrap();

        // Extend server-side dataset with new nodes
        let server_repo = server_harness.cli_dataset_registry();

        let server_dataset_ref = make_dataset_ref(server_account_name.as_ref(), "foo");
        let server_dataset_handle = server_repo
            .resolve_dataset_handle_by_ref(&server_dataset_ref)
            .await
            .unwrap();

        let commit_dataset_event = server_harness.cli_commit_dataset_event_use_case();
        commit_dataset_event
            .execute(
                &server_dataset_handle,
                odf::MetadataEvent::SetInfo(
                    MetadataFactory::set_info()
                        .description("updated description")
                        .build(),
                ),
            )
            .await
            .unwrap();

        let server_commit_result = commit_add_data_event(
            server_repo.as_ref(),
            &server_dataset_ref,
            &server_dataset_layout,
            None,
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
            server_create_result,
            server_commit_result,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
