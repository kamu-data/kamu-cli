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
    make_dataset_ref,
    write_dataset_alias,
    ClientSideHarness,
    ServerSideHarness,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct SmartPushExistingDivergedDatasetScenario<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_layout: DatasetLayout,
    pub client_dataset_layout: DatasetLayout,
    pub server_dataset_ref: DatasetRefRemote,
    pub client_dataset_ref: DatasetRef,
    pub client_precompaction_result: CommitResult,
    pub client_compaction_result: CompactionResult,
}

impl<TServerHarness: ServerSideHarness> SmartPushExistingDivergedDatasetScenario<TServerHarness> {
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
                    .name(DatasetAlias::new(
                        client_account_name.clone(),
                        DatasetName::new_unchecked("foo"),
                    ))
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .push_event(MetadataFactory::set_data_schema().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap();

        let client_dataset_layout =
            client_harness.dataset_layout(&client_create_result.dataset_handle.id, "foo");

        let client_dataset_ref = make_dataset_ref(client_account_name.as_ref(), "foo");

        // Generate a few blocks of random data
        let mut commit_result: Option<CommitResult> = None;
        for _ in 0..3 {
            commit_result = Some(
                commit_add_data_event(
                    client_harness.dataset_registry().as_ref(),
                    &client_dataset_ref,
                    &client_dataset_layout,
                    commit_result.map(|r| r.new_head),
                )
                .await,
            );
        }
        let foo_name = DatasetName::new_unchecked("foo");

        let server_dataset_layout = server_harness.dataset_layout(&DatasetHandle::new(
            client_create_result.dataset_handle.id.clone(),
            DatasetAlias::new(server_account_name.clone(), foo_name.clone()),
        ));

        // Hard folder synchronization
        copy_dataset_files(&client_dataset_layout, &server_dataset_layout).unwrap();

        write_dataset_alias(
            &server_dataset_layout,
            &DatasetAlias::new(server_account_name.clone(), foo_name.clone()),
        )
        .await;

        let client_dataset = client_harness
            .dataset_registry()
            .get_dataset_by_handle(&client_create_result.dataset_handle);

        // Compact at client side
        let compaction_planner = client_harness.compaction_planner();
        let compaction_execution_service = client_harness.compaction_executor();

        let client_compaction_result = compaction_execution_service
            .execute(
                client_dataset.clone(),
                compaction_planner
                    .plan_compaction(client_dataset, CompactionOptions::default(), None)
                    .await
                    .unwrap(),
                None,
            )
            .await
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
            client_dataset_ref,
            client_precompaction_result: commit_result.unwrap(),
            client_compaction_result,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
