// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt, TokenValidateError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_password_predefined_successful(
    kamu_api_server_client: KamuApiServerClient,
) {
    kamu_api_server_client
        .graphql_api_call_assert(
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_enabled_methods(kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client
        .graphql_api_call_assert(
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_dummy_github(kamu_api_server_client: KamuApiServerClient) {
    // 1. No user
    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                query {
                  accounts {
                    byName(name: "e2e-user") {
                      accountName
                    }
                  }
                }
                "#,
            ),
            Ok(indoc::indoc!(
                r#"
                {
                  "accounts": {
                    "byName": null
                  }
                }
                "#,
            )),
        )
        .await;

    // 2. Create a user
    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                mutation {
                  auth {
                    login(loginMethod: "oauth_github", loginCredentialsJson: "{\"login\":\"e2e-user\"}") {
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
                        "accountName": "e2e-user"
                      }
                    }
                  }
                }
                "#,
            )),
        )
        .await;

    // 3. Verify that the user has been created
    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                query {
                  accounts {
                    byName(name: "e2e-user") {
                      accountName
                    }
                  }
                }
                "#,
            ),
            Ok(indoc::indoc!(
                r#"
                {
                  "accounts": {
                    "byName": {
                      "accountName": "e2e-user"
                    }
                  }
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_kamu_access_token_middleware(mut kamu_api_server_client: KamuApiServerClient) {
    // 1. Grub a JWT
    let login_response = kamu_api_server_client
     .graphql_api_call(indoc::indoc!(
        r#"
        mutation {
            auth {
                login(loginMethod: "password", loginCredentialsJson: "{\"login\":\"kamu\",\"password\":\"kamu\"}") {
                    accessToken,
                    account {
                        id
                    }
                }
            }
        }
        "#,
         ))
     .await;
    let access_token = login_response["auth"]["login"]["accessToken"]
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap();

    kamu_api_server_client.set_token(Some(access_token));

    let account_id = login_response["auth"]["login"]["account"]["id"]
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap();

    // 2. Grub a kamu access token
    let create_token_response = kamu_api_server_client
        .graphql_api_call(
            indoc::indoc!(
                r#"
            mutation {
                auth {
                    createAccessToken (accountId: "<account_id>", tokenName: "foo") {
                        __typename
                        message
                        ... on CreateAccessTokenResultSuccess {
                            token {
                                id,
                                name,
                                composed
                            }
                        }
                    }
                }
            }
            "#,
            )
            .replace("<account_id>", account_id.as_str())
            .as_str(),
        )
        .await;
    let kamu_token = create_token_response["auth"]["createAccessToken"]["token"]["composed"]
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap();

    kamu_api_server_client.set_token(Some(kamu_token));

    // 3. Create dataset from snapshot with new token
    kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_token_validate(mut kamu_api_server_client: KamuApiServerClient) {
    assert_matches!(
        kamu_api_server_client.auth().token_validate().await,
        Err(TokenValidateError::Unauthorized)
    );

    kamu_api_server_client.auth().login_as_kamu().await;

    assert_matches!(kamu_api_server_client.auth().token_validate().await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
