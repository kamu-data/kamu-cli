// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::{
    domain::{DatasetRepositoryExt, PullResult},
    testing::MetadataFactory,
};
use opendatafabric::{DatasetKind, DatasetRefAny, DatasetRefRemote};
use std::time;

use crate::harness::{client_side_harness, server_side_harness};

/////////////////////////////////////////////////////////////////////////////////////////

macro_rules! await_client_server_flow {
    ($api_server_handle: expr, $client_handle: expr) => {
        tokio::select! {
            _ = tokio::time::sleep(time::Duration::from_secs(60)) => panic!("test timeout!"),
            _ = $api_server_handle => panic!("server-side aborted"),
            _ = $client_handle => {} // Pass, do nothing
        }
    };
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_smart_pull_new_dataset() {
    let server_harness = server_side_harness();

    let server_repo = server_harness.dataset_repository();
    let create_result = server_repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let client_harness = client_side_harness();

    let foo_odf_url = server_harness.dataset_url("foo");
    let foo_dataset_ref = DatasetRefRemote::from(&foo_odf_url);

    let api_server_handle = async { server_harness.api_server_run().await };
    let client_handle = async {
        let pull_result = client_harness
            .pull_dataset(DatasetRefAny::from(foo_dataset_ref))
            .await;
        assert_eq!(
            PullResult::Updated {
                old_head: None,
                new_head: create_result.head,
                num_blocks: 2
            },
            pull_result
        )
    };

    await_client_server_flow!(api_server_handle, client_handle)
}

/////////////////////////////////////////////////////////////////////////////////////////
