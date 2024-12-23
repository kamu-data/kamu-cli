// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_query_mut_create_empty_returns_correct_alias_mt(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    let token = kamu_api_server_client.auth().login_as_e2e_user().await;
    kamu_api_server_client.set_token(Some(token));

    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                mutation {
                  datasets {
                    createEmpty(datasetKind: ROOT, datasetAlias: "empty-root-dataset") {
                      message
                      ... on CreateDatasetResultSuccess {
                        dataset {
                          alias
                          owner {
                            accountName
                          }
                        }
                      }
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
                      "dataset": {
                        "alias": "e2e-user/empty-root-dataset",
                        "owner": {
                          "accountName": "e2e-user"
                        }
                      },
                      "message": "Success"
                    }
                  }
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_query_mut_create_empty_returns_correct_alias_st(
    mut kamu_api_server_client: KamuApiServerClient,
) {
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
    let token = login_response["auth"]["login"]["accessToken"]
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap();
    kamu_api_server_client.set_token(Some(token));

    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                mutation {
                  datasets {
                    createEmpty(datasetKind: ROOT, datasetAlias: "empty-root-dataset") {
                      message
                      ... on CreateDatasetResultSuccess {
                        dataset {
                          alias
                          owner {
                            accountName
                          }
                        }
                      }
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
                      "dataset": {
                        "alias": "empty-root-dataset",
                        "owner": {
                          "accountName": "kamu"
                        }
                      },
                      "message": "Success"
                    }
                  }
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
