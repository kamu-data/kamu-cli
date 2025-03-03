// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::StatusCode;

use crate::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unknown_uri_returns_not_found() {
    let harness = UnknownHandlerHarness::new().await;

    let client = async move {
        let cl = reqwest::Client::new();

        let res = cl
            .get(format!("{}unknown-uri", harness.root_url))
            .send()
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        assert_eq!(res.text().await.unwrap(), "Not Found");
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct UnknownHandlerHarness {
    root_url: url::Url,
    server_harness: ServerSideLocalFsHarness,
}

impl UnknownHandlerHarness {
    async fn new() -> Self {
        let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: kamu_core::TenancyConfig::SingleTenant,
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
