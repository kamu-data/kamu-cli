// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_swagger_page_st(kamu_api_server_client: KamuApiServerClient) {
    test_swagger_page(kamu_api_server_client).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_swagger_page_mt(kamu_api_server_client: KamuApiServerClient) {
    test_swagger_page(kamu_api_server_client).await;
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_swagger_schema_st(kamu_api_server_client: KamuApiServerClient) {
    let mut schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    schema_path.push("../../../../../resources/openapi.json");

    test_swagger_schema(kamu_api_server_client, schema_path).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_swagger_schema_mt(kamu_api_server_client: KamuApiServerClient) {
    let mut schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    schema_path.push("../../../../../resources/openapi-mt.json");

    test_swagger_schema(kamu_api_server_client, schema_path).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_swagger_page(kamu_api_server_client: KamuApiServerClient) {
    let page = kamu_api_server_client.swagger().main_page().await;

    assert!(
        page.contains("<title>Kamu REST API Reference</title>"),
        "{}",
        indoc::formatdoc!(
            r#"
            Page content:
            {page}
            "#
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_swagger_schema(kamu_api_server_client: KamuApiServerClient, schema_path: PathBuf) {
    let actual_schema = kamu_api_server_client.swagger().schema().await;
    let schema_from_resources: serde_json::Value = std::fs::read_to_string(schema_path)
        .unwrap()
        .parse()
        .unwrap();

    pretty_assertions::assert_eq!(schema_from_resources, actual_schema);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
