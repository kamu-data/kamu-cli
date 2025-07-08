// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_cli_e2e_common::{
    GraphQLResponseExt,
    KamuApiServerClient,
    KamuApiServerClientExt,
    LoginError,
    TokenValidateError,
};
use serde_json::json;

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
                    login(loginMethod: PASSWORD, loginCredentialsJson: "{\"login\":\"kamu\",\"password\":\"kamu.dev\"}") {
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

pub async fn test_login_enabled_providers_st(kamu_api_server_client: KamuApiServerClient) {
    let res = kamu_api_server_client
        .graphql_api_call_ex(async_graphql::Request::new(indoc::indoc!(
            r#"
            query {
              auth {
                enabledProviders
              }
            }
            "#,
        )))
        .await;
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "auth": {
                "enabledProviders": [
                    async_graphql::Name::new("PASSWORD"),
                ]
            }
        }),
        res.data,
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_enabled_providers_mt(kamu_api_server_client: KamuApiServerClient) {
    let res = kamu_api_server_client
        .graphql_api_call_ex(async_graphql::Request::new(indoc::indoc!(
            r#"
            query {
              auth {
                enabledProviders
              }
            }
            "#,
        )))
        .await;
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "auth": {
                "enabledProviders": [
                    async_graphql::Name::new("OAUTH_GITHUB"),
                    async_graphql::Name::new("PASSWORD"),
                    async_graphql::Name::new("WEB3_WALLET"),
                ]
            }
        }),
        res.data,
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_dummy_github(kamu_api_server_client: KamuApiServerClient) {
    // Create a user
    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                mutation {
                  auth {
                    login(loginMethod: OAUTH_GITHUB, loginCredentialsJson: "{\"login\":\"e2e-user\"}") {
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

    // Verify that the user has been created
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
     .graphql_api_call(
      indoc::indoc!(
          r#"
          mutation {
              auth {
                  login(loginMethod: PASSWORD, loginCredentialsJson: "{\"login\":\"kamu\",\"password\":\"kamu.dev\"}") {
                      accessToken,
                      account {
                          id
                      }
                  }
              }
          }
          "#,
         ),
         None)
     .await
     .data();

    let access_token = login_response["auth"]["login"]["accessToken"]
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap();
    let account_id_str = login_response["auth"]["login"]["account"]["id"]
        .as_str()
        .unwrap();
    let account_id = odf::AccountID::from_did_str(account_id_str).unwrap();

    kamu_api_server_client
        .auth()
        .set_logged_as(access_token, account_id.clone());

    // 2. Grub a kamu access token
    let create_token_response = kamu_api_server_client
        .graphql_api_call(
            indoc::indoc!(
                r#"
            mutation {
                accounts {
                    byId(accountId: "<account_id>") {
                        createAccessToken (tokenName: "foo") {
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
            }
            "#,
            )
            .replace("<account_id>", account_id_str)
            .as_str(),
            None,
        )
        .await
        .data();
    let kamu_token = create_token_response["accounts"]["byId"]["createAccessToken"]["token"]
        ["composed"]
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap();

    kamu_api_server_client
        .auth()
        .set_logged_as(kamu_token, account_id);

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

pub async fn test_login_via_rest_password(mut kamu_api_server_client: KamuApiServerClient) {
    let login_credentials_json = json!({
        "login": "kamu",
        "password": "kamu.dev"
    });

    assert_matches!(
        kamu_api_server_client
            .auth()
            .login_via_rest("password", login_credentials_json)
            .await,
        Ok(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_via_rest_dummy_github(mut kamu_api_server_client: KamuApiServerClient) {
    let login_credentials_json = serde_json::Value::Null;

    assert_matches!(
        kamu_api_server_client
            .auth()
            .login_via_rest("oauth_github", login_credentials_json)
            .await,
        Ok(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_login_via_rest_unauthorized(mut kamu_api_server_client: KamuApiServerClient) {
    let login_credentials_json = json!({
        "login": "kamu",
        "password": "wrong-password"
    });

    assert_matches!(
        kamu_api_server_client
            .auth()
            .login_via_rest("password", login_credentials_json)
            .await,
        Err(LoginError::Unauthorized)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
