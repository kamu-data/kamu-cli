// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_core::TenancyConfig;
use odf::metadata::testing::MetadataFactory;
use serde_json::json;

use crate::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_dataset_info_by_id() {
    let harness = DatasetInfoHarness::new(TenancyConfig::SingleTenant).await;

    let dataset_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness
        .server_harness
        .cli_create_dataset_use_case()
        .execute(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .system_time(Utc::now())
                .build_typed(),
            Default::default(),
        )
        .await
        .unwrap();

    let client = async move {
        let cl = reqwest::Client::new();

        let res = cl
            .get(format!(
                "{}datasets/{}",
                harness.root_url, create_result.dataset_handle.id
            ))
            .header(
                "Authorization",
                format!("Bearer {}", odf::dataset::DUMMY_ODF_ACCESS_TOKEN),
            )
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
            })
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_dataset_info_by_id_not_found_err() {
    let harness = DatasetInfoHarness::new(TenancyConfig::SingleTenant).await;

    let client = async move {
        let cl = reqwest::Client::new();
        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

        let res = cl
            .get(format!("{}datasets/{dataset_id}", harness.root_url))
            .header(
                "Authorization",
                format!("Bearer {}", odf::dataset::DUMMY_ODF_ACCESS_TOKEN),
            )
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::NOT_FOUND, res.status());
        pretty_assertions::assert_eq!(
            json!({
                "message": "Not Found"
            }),
            res.json::<serde_json::Value>().await.unwrap()
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
    async fn new(tenancy_config: TenancyConfig) -> Self {
        let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
