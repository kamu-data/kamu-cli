// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::testing::MetadataFactory;
use opendatafabric::*;

use crate::harness::{
    commit_add_data_event,
    copy_dataset_files,
    make_dataset_ref,
    write_dataset_alias,
    ClientSideHarness,
    ServerSideHarness,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPushExistingRefCollisionScenarion<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_ref: DatasetRefRemote,
    pub client_dataset_ref: DatasetRef,
}

impl<TServerHarness: ServerSideHarness> SmartPushExistingRefCollisionScenarion<TServerHarness> {
    pub async fn prepare(
        client_harness: ClientSideHarness,
        server_harness: TServerHarness,
    ) -> Self {
        let client_account_name = client_harness.operating_account_name();
        let server_account_name = server_harness.operating_account_name();

        let client_repo = client_harness.dataset_repository();

        let client_create_result = client_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(DatasetAlias::new(
                        client_account_name.clone(),
                        DatasetName::new_unchecked("foo"),
                    ))
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .push_event(MetadataFactory::set_data_schema().build())
                    .build(),
            )
            .await
            .unwrap();

        let client_dataset_layout =
            client_harness.dataset_layout(&client_create_result.dataset_handle.id, "foo");

        let foo_name = DatasetName::new_unchecked("foo");

        let server_dataset_layout = server_harness.dataset_layout(&DatasetHandle::new(
            client_create_result.dataset_handle.id.clone(),
            DatasetAlias::new(server_account_name.clone(), foo_name.clone()),
        ));

        let client_dataset_ref: DatasetRef = make_dataset_ref(&client_account_name, "foo");
        commit_add_data_event(
            client_repo.as_ref(),
            &client_dataset_ref,
            &client_dataset_layout,
            None,
        )
        .await;

        // Hard folder synchronization
        copy_dataset_files(&client_dataset_layout, &server_dataset_layout).unwrap();

        write_dataset_alias(
            &server_dataset_layout,
            &DatasetAlias::new(client_account_name.clone(), foo_name.clone()),
        )
        .await;

        let bar_name = DatasetName::new_unchecked("bar");
        let server_alias = DatasetAlias::new(server_account_name, bar_name);
        let server_odf_url = server_harness.dataset_url(&server_alias);
        let server_dataset_ref = DatasetRefRemote::from(&server_odf_url);
        Self {
            client_harness,
            server_harness,
            server_dataset_ref,
            client_dataset_ref,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
