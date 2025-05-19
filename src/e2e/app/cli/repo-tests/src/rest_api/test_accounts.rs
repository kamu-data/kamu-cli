// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use indoc::indoc;
use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME};
use kamu_adapter_graphql::traits::ResponseExt;
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
    // First, try to create an account with a user without permissions
    kamu_api_server_client.auth().login_as_e2e_user().await;

    let res = kamu_api_server_client
        .graphql_api_call_ex(create_account_request("foo"))
        .await;
    pretty_assertions::assert_eq!(
        ["Account is not authorized to provision accounts"],
        *res.error_messages(),
        "{res:?}"
    );

    kamu_api_server_client.auth().logout();
    kamu_api_server_client.auth().login_as_kamu().await;

    let res = kamu_api_server_client
        .graphql_api_call_ex(create_account_request("foo"))
        .await;
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "accounts": {
                "createAccount": {
                    "account": {
                        "accountName": "foo",
                        "accountType": "USER",
                        "displayName": "foo"
                    },
                    "message": "Account created"

                }
            }
        }),
        res.data,
        "{res:?}"
    );

    let res = kamu_api_server_client
        .graphql_api_call_ex(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($accountName: AccountName, $newPassword: AccountPassword!) {
                  accounts {
                    byName(accountName: $accountName) {
                      modifyPassword(password: $newPassword) {
                        message
                      }
                    }
                  }
                }
                "#,
            ))
            .variables(async_graphql::Variables::from_value(
                async_graphql::value!({
                    "accountName": "foo",
                    "newPassword": "foo_password",
                }),
            )),
        )
        .await;
    pretty_assertions::assert_eq!(
        async_graphql::value!({
            "accounts": {
                "byName": {
                    "modifyPassword": {
                        "message": "Password modified"
                    }
                }
            }
        }),
        res.data,
        "{res:?}"
    );

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
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_account_request(new_account_name: &str) -> async_graphql::Request {
    async_graphql::Request::new(indoc!(
        r#"
        mutation ($newAccountName: AccountName!) {
          accounts {
            createAccount(accountName: $newAccountName) {
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
        "#,
    ))
    .variables(async_graphql::Variables::from_value(
        async_graphql::value!({
            "newAccountName": new_account_name,
        }),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
