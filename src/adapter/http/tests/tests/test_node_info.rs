// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::TenancyConfig;
use serde_json::json;

use crate::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_node_info_single_tenant() {
    let harness = NodeInfoHarness::new(TenancyConfig::SingleTenant).await;

    let client = async move {
        let cl = reqwest::Client::new();

        let res = cl
            .get(format!("{}info", harness.root_url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        pretty_assertions::assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({
                "isMultiTenant": false
            })
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

#[test_log::test(tokio::test)]
async fn test_node_info_multi_tenant() {
    let harness = NodeInfoHarness::new(TenancyConfig::MultiTenant).await;

    let client = async move {
        let cl = reqwest::Client::new();

        let res = cl
            .get(format!("{}info", harness.root_url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();

        pretty_assertions::assert_eq!(
            res.json::<serde_json::Value>().await.unwrap(),
            json!({
                "isMultiTenant": true
            })
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct NodeInfoHarness {
    root_url: url::Url,
    server_harness: ServerSideLocalFsHarness,
}

impl NodeInfoHarness {
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
