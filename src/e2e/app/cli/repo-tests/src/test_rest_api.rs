// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::KamuApiServerClient;
use reqwest::{Method, StatusCode};

////////////////////////////////////////////////////////////////////////////////

// TODO: add for DBs after fixes
pub async fn test_rest_api_request_dataset_tail(kamu_api_server_client: KamuApiServerClient) {
    // 1. Grub a token
    let login_response = kamu_api_server_client
        .graphql_api_call(indoc::indoc!(
                r#"
                mutation {
                  auth {
                    login(loginMethod: "password", loginCredentialsJson: "{\"login\":\"kamu\",\"password\":\"kamu\"}") {
                      accessToken
                    }
                  }
                }
                "#,
            ), None)
        .await;
    let token = login_response["auth"]["login"]["accessToken"]
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap();

    // 2. Create an empty dataset
    kamu_api_server_client
        .graphql_api_call_assert_with_token(
            token.clone(),
            indoc::indoc!(
                r#"
                mutation {
                  datasets {
                    createEmpty(datasetKind: ROOT, datasetAlias: "empty-root-dataset") {
                      message
                    }
                  }
                }
                "#,
            ),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "createEmpty": {
                      "message": "Success"
                    }
                  }
                }
                "#,
            )),
        )
        .await;

    // 3. Get dataset tail
    kamu_api_server_client
        .rest_api_call_assert(
            Method::GET,
            "empty-root-dataset/tail?limit=10",
            None,
            StatusCode::OK,
            // TODO: add expected body
            None,
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////
