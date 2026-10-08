// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf::metadata::testing::MetadataFactory;

use crate::harness::{ClientSideHarness, ServerSideHarness};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPullExistingDifferentDatasetFailsScenario<TServerHarness: ServerSideHarness>
{
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_ref: odf::DatasetRefRemote,
    pub server_dataset_id: odf::DatasetID,
    pub client_dataset_id: odf::DatasetID,
}

impl<TServerHarness: ServerSideHarness>
    SmartPullExistingDifferentDatasetFailsScenario<TServerHarness>
{
    pub async fn prepare(
        client_harness: ClientSideHarness,
        server_harness: TServerHarness,
    ) -> Self {
        let foo_name = odf::DatasetName::new_unchecked("foo");

        // Server and client each create their own "foo", so the IDs differ
        let server_alias =
            odf::DatasetAlias::new(server_harness.operating_account_name(), foo_name.clone());
        let server_create_result = server_harness
            .cli_create_dataset_from_snapshot_use_case()
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(server_alias.clone())
                    .kind(odf::DatasetKind::Root)
                    .push_event(MetadataFactory::set_data_schema().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap();

        let client_create_result = client_harness
            .create_dataset_from_snapshot()
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(odf::DatasetAlias::new(
                        client_harness.operating_account_name(),
                        foo_name,
                    ))
                    .kind(odf::DatasetKind::Root)
                    .push_event(MetadataFactory::set_data_schema().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap();

        let server_odf_url = server_harness.dataset_url(&server_alias);
        let server_dataset_ref = odf::DatasetRefRemote::from(&server_odf_url);

        Self {
            client_harness,
            server_harness,
            server_dataset_ref,
            server_dataset_id: server_create_result.dataset_handle.id,
            client_dataset_id: client_create_result.dataset_handle.id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
