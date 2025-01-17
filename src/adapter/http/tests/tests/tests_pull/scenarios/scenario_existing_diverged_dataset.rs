// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
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

pub(crate) struct SmartPullExistingDivergedDatasetScenario<TServerHarness: ServerSideHarness> {
    pub client_harness: ClientSideHarness,
    pub server_harness: TServerHarness,
    pub server_dataset_layout: DatasetLayout,
    pub client_dataset_layout: DatasetLayout,
    pub server_dataset_ref: odf::DatasetRefRemote,
    pub server_precompaction_result: odf::dataset::CommitResult,
    pub server_compaction_result: CompactionResult,
}

impl<TServerHarness: ServerSideHarness> SmartPullExistingDivergedDatasetScenario<TServerHarness> {
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

        let server_dataset_ref = make_dataset_ref(server_account_name.as_ref(), "foo");

        // Generate a few blocks of random data
        let mut commit_result: Option<odf::dataset::CommitResult> = None;
        for _ in 0..3 {
            commit_result = Some(
                commit_add_data_event(
                    server_harness.cli_dataset_registry().as_ref(),
                    &server_dataset_ref,
                    &server_dataset_layout,
                    commit_result.map(|r| r.new_head),
                )
                .await,
            );
        }

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

        let server_dataset = server_harness
            .cli_dataset_registry()
            .get_dataset_by_handle(&server_create_result.dataset_handle);

        let compaction_planner = server_harness.cli_compaction_planner();
        let compaction_execution_service = server_harness.cli_compaction_executor();

        let server_compaction_result = compaction_execution_service
            .execute(
                server_dataset.clone(),
                compaction_planner
                    .plan_compaction(server_dataset, CompactionOptions::default(), None)
                    .await
                    .unwrap(),
                None,
            )
            .await
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
            server_precompaction_result: commit_result.unwrap(),
            server_compaction_result,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
