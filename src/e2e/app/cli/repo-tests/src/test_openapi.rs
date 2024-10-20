// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use kamu_cli_e2e_common::KamuApiServerClient;
use reqwest::Method;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_openapi(kamu_api_server_client: KamuApiServerClient, schema_path: PathBuf) {
    let response = kamu_api_server_client
        .rest_api_call(None, Method::GET, "/openapi.json", None)
        .await
        .error_for_status()
        .unwrap();

    let schema: serde_json::Value = response.json().await.unwrap();
    let schema = serde_json::to_string_pretty(&schema).unwrap();
    std::fs::write(&schema_path, &schema).unwrap();
}

pub async fn test_openapi_st(kamu_api_server_client: KamuApiServerClient) {
    let mut schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    schema_path.push("../../../../../resources/openapi.json");
    test_openapi(kamu_api_server_client, schema_path).await;
}

pub async fn test_openapi_mt(kamu_api_server_client: KamuApiServerClient) {
    let mut schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    schema_path.push("../../../../../resources/openapi-mt.json");
    test_openapi(kamu_api_server_client, schema_path).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
