// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::testing::MetadataFactory;
use kamu::DatasetLayout;
use opendatafabric::*;

use crate::harness::{
    commit_add_data_event,
    copy_dataset_files,
    make_dataset_ref,
    write_dataset_alias,
    ClientSideHarness,
    ServerSideHarness,
};

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPullExistingUpToDateDatasetScenario<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_layout: DatasetLayout,
    pub client_dataset_layout: DatasetLayout,
    pub server_dataset_ref: DatasetRefRemote,
}

impl<TServerHarness: ServerSideHarness> SmartPullExistingUpToDateDatasetScenario<TServerHarness> {
    pub async fn prepare(
        client_harness: ClientSideHarness,
        server_harness: TServerHarness,
    ) -> Self {
        let server_account_name = server_harness.operating_account_name();

        let server_repo = server_harness.cli_dataset_repository();
        let server_create_result = server_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(DatasetAlias::new(
                        server_account_name.clone(),
                        DatasetName::new_unchecked("foo"),
                    ))
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .push_event(MetadataFactory::set_data_schema().build())
                    .build(),
            )
            .await
            .unwrap();

        let server_dataset_layout =
            server_harness.dataset_layout(&server_create_result.dataset_handle);

        commit_add_data_event(
            server_repo.as_ref(),
            &make_dataset_ref(&server_account_name, "foo"),
            &server_dataset_layout,
        )
        .await;

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

        let server_alias = DatasetAlias::new(server_account_name, foo_name);
        let server_odf_url = server_harness.dataset_url(&server_alias);
        let server_dataset_ref = DatasetRefRemote::from(&server_odf_url);

        Self {
            client_harness,
            server_harness,
            server_dataset_layout,
            client_dataset_layout,
            server_dataset_ref,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
