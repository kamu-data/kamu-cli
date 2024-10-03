// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu::testing::MetadataFactory;
use opendatafabric::{DatasetAlias, DatasetID, DatasetKind, DatasetName};
use serde_json::json;

use crate::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_dataset_info_by_id() {
    let harness = DatasetInfoHarness::new(false).await;

    let dataset_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let create_result = harness
        .server_harness
        .cli_create_dataset_use_case()
        .execute(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .system_time(Utc::now())
                .build_typed(),
            Default::default(),
        )
        .await
        .unwrap();

    let client = async move {
        let cl = reqwest::Client::new();

        let res = cl
            .get(&format!(
                "{}datasets/{}",
                harness.root_url, create_result.dataset_handle.id
            ))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        pretty_assertions::assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({
                "id": create_result.dataset_handle.id,
                "datasetName": create_result.dataset_handle.alias.dataset_name,
                "accountName": create_result.dataset_handle.alias.account_name,
            })
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

#[test_log::test(tokio::test)]
async fn test_get_dataset_info_by_id_not_found_err() {
    let harness = DatasetInfoHarness::new(false).await;

    let client = async move {
        let cl = reqwest::Client::new();
        let dataset_id = DatasetID::new_seeded_ed25519(b"foo");

        let res = cl
            .get(&format!("{}datasets/{dataset_id}", harness.root_url))
            .send()
            .await
            .unwrap();

        assert_eq!(404, res.status());
        assert_eq!(
            format!("Dataset not found: {dataset_id}"),
            res.text().await.unwrap()
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetInfoHarness {
    root_url: url::Url,
    server_harness: ServerSideLocalFsHarness,
}

impl DatasetInfoHarness {
    async fn new(is_multi_tenant: bool) -> Self {
        let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            multi_tenant: is_multi_tenant,
            authorized_writes: true,
            base_catalog: None,
        })
        .await;

        let root_url = url::Url::parse(
            format!("http://{}", server_harness.api_server_addr()).trim_end_matches('/'),
        )
        .unwrap();

        Self {
            root_url,
            server_harness,
        }
    }
}
