// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use odf::metadata::testing::MetadataFactory;

use crate::harness::{
    copy_dataset_files,
    make_dataset_ref,
    write_dataset_alias,
    ClientSideHarness,
    ServerSideHarness,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPullExistingAdvancedDatasetFailsScenario<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_ref: odf::DatasetRefRemote,
}

impl<TServerHarness: ServerSideHarness>
    SmartPullExistingAdvancedDatasetFailsScenario<TServerHarness>
{
    pub async fn prepare(
        client_harness: ClientSideHarness,
        server_harness: TServerHarness,
    ) -> Self {
        let client_account_name = client_harness.operating_account_name();
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
            client_harness.dataset_layout(&server_create_result.dataset_handle.id);

        // Hard folder synchronization
        copy_dataset_files(&server_dataset_layout, &client_dataset_layout).unwrap();

        let foo_name = odf::DatasetName::new_unchecked("foo");
        write_dataset_alias(
            &client_dataset_layout,
            &odf::DatasetAlias::new(client_account_name.clone(), foo_name.clone()),
        )
        .await;

        client_harness
            .dataset_entry_writer()
            .create_entry(
                &server_create_result.dataset_handle.id,
                &client_harness.client_account_id(),
                &client_harness.client_account_name(),
                &server_create_result.dataset_handle.alias.dataset_name,
                odf::DatasetKind::Root,
            )
            .await
            .unwrap();

        client_harness
            .dataset_reference_service()
            .set_reference(
                &server_create_result.dataset_handle.id,
                &odf::BlockRef::Head,
                None,
                &server_create_result.head,
            )
            .await
            .unwrap();

        // Extend client-side dataset with new node
        let client_registry = client_harness.dataset_registry();
        client_registry
            .get_dataset_by_ref(&make_dataset_ref(client_account_name.as_ref(), "foo"))
            .await
            .unwrap()
            .commit_event(
                odf::MetadataEvent::SetInfo(
                    MetadataFactory::set_info()
                        .description("updated description")
                        .build(),
                ),
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap();

        let server_alias = odf::DatasetAlias::new(server_account_name, foo_name);
        let server_odf_url = server_harness.dataset_url(&server_alias);
        let server_dataset_ref = odf::DatasetRefRemote::from(&server_odf_url);

        Self {
            client_harness,
            server_harness,
            server_dataset_ref,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
