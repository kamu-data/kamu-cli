// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::KamuApiServerClient;

////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_password_predefined_successful(
    kamu_api_server_client: KamuApiServerClient,
) {
    kamu_api_server_client
        .api_call_assert(
            indoc::indoc!(
                r#"
                mutation {
                  auth {
                    login(loginMethod: "password", loginCredentialsJson: "{\"login\":\"kamu\",\"password\":\"kamu\"}") {
                      account {
                        accountName
                      }
                    }
                  }
                }
                "#,
            ),
            Ok(indoc::indoc!(
                r#"
                {
                  "auth": {
                    "login": {
                      "account": {
                        "accountName": "kamu"
                      }
                    }
                  }
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_enabled_methods(kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client
        .api_call_assert(
            indoc::indoc!(
                r#"
                query {
                  auth {
                    enabledLoginMethods
                  }
                }
                "#,
            ),
            Ok(indoc::indoc!(
                r#"
                {
                  "auth": {
                    "enabledLoginMethods": [
                      "password"
                    ]
                  }
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////
