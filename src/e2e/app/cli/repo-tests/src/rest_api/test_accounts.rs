// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME};
use kamu_cli_e2e_common::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_accounts_me_kamu_user(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    assert_matches!(
        kamu_api_server_client.account().me().await,
        Ok(response)
            if response.id == *DEFAULT_ACCOUNT_ID
                && response.account_name == *DEFAULT_ACCOUNT_NAME
    );
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_accounts_me_e2e_user(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_e2e_user().await;

    assert_matches!(
        kamu_api_server_client.account().me().await,
        Ok(response)
            if response.account_name == *E2E_USER_ACCOUNT_NAME
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_accounts_me_unauthorized(kamu_api_server_client: KamuApiServerClient) {
    assert_matches!(
        kamu_api_server_client.account().me().await,
        Err(AccountMeError::Unauthorized)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_account_and_modify_password(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    // First try to create account with user without permissions
    kamu_api_server_client.auth().login_as_e2e_user().await;
    let owner_account_info = kamu_api_server_client.account().me().await.unwrap();

    let result = kamu_api_server_client
        .graphql_api_call(&create_account_query(&owner_account_info.id, "foo"), None)
        .await;

    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().first().unwrap().message,
        "Account does not have permission to provision accounts"
    );

    kamu_api_server_client.auth().logout();
    kamu_api_server_client.auth().login_as_kamu().await;
    let owner_account_info = kamu_api_server_client.account().me().await.unwrap();

    kamu_api_server_client
        .graphql_api_call_assert(
            &create_account_query(&owner_account_info.id, "foo"),
            Ok(indoc::indoc!(
                r#"
                {
                  "accounts": {
                    "byId": {
                      "createAccount": {
                        "account": {
                          "accountName": "foo",
                          "accountType": "USER",
                          "displayName": "foo"
                        },
                        "message": "Account created"
                      }
                    }
                  }
                }
            "#,
            )),
        )
        .await;

    kamu_api_server_client
        .graphql_api_call_assert(
            &indoc::indoc!(
                r#"
                mutation {
                    accounts {
                        byId (accountId: $accountId) {
                            modifyPassword(accountName: "foo", password: "foo_password") {
                                message
                            }
                        }
                    }
                }
                "#
            )
            .replace("$accountId", &format!("\"{}\"", owner_account_info.id)),
            Ok(indoc::indoc!(
                r#"
                {
                  "accounts": {
                    "byId": {
                      "modifyPassword": {
                        "message": "Password modified"
                      }
                    }
                  }
                }
            "#,
            )),
        )
        .await;

    kamu_api_server_client.auth().logout();
    kamu_api_server_client
        .auth()
        .login_with_password("foo", "foo_password")
        .await;
    assert_matches!(
        kamu_api_server_client.account().me().await,
        Ok(response)
            if response.account_name == odf::AccountName::new_unchecked("foo")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_account_query(account_id: &odf::AccountID, account_name: &str) -> String {
    indoc::indoc!(
        r#"
        mutation {
            accounts {
                byId (accountId: $accountId) {
                    createAccount(accountName: "$accountName") {
                        ... on CreateAccountSuccess {
                            message
                            account {
                                accountName
                                displayName
                                accountType
                            }
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("$accountId", &format!("\"{account_id}\""))
    .replace("$accountName", account_name)
}
