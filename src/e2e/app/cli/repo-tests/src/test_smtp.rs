// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli::testing::Kamu;
use kamu_cli_e2e_common::KamuApiServerClient;
use reqwest::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smpt_push_pull_sequence(kamu_api_server_client: KamuApiServerClient) {
    // 1. Grub a token
    let token = {
        let login_response = kamu_api_server_client
            .graphql_api_call(
                indoc::indoc!(
                r#"
                mutation {
                  auth {
                    login(loginMethod: "oauth_github", loginCredentialsJson: "{\"login\":\"e2e-user\"}") {
                      accessToken
                    }
                  }
                }
                "#,
            ),
                None,
            )
            .await;

        login_response["auth"]["login"]["accessToken"]
            .as_str()
            .map(ToOwned::to_owned)
            .unwrap()
    };

    let kamu_api_server_dataset_endpoint = {
        let base_url = kamu_api_server_client.get_base_url();

        let mut dataset_endpoint = Url::parse("odf+http://host").unwrap();

        dataset_endpoint.set_host(base_url.host_str()).unwrap();
        dataset_endpoint.set_port(base_url.port()).unwrap();

        dataset_endpoint
            .join("e2e-user/player-scores")
            .unwrap()
            .to_string()
    };

    // 2. Pushing the dataset to the API server
    {
        let kamu_in_push_workspace = Kamu::new_workspace_tmp().await;

        // 2.1. Add the dataset
        {
            let dataset_path = kamu_in_push_workspace
                .workspace_path()
                .join("player-scores.yaml");

            std::fs::write(
                dataset_path.clone(),
                indoc::indoc!(
                    r#"
                    kind: DatasetSnapshot
                    version: 1
                    content:
                      name: player-scores
                      kind: Root
                      metadata:
                        - kind: AddPushSource
                          sourceName: default
                          read:
                            kind: NdJson
                            schema:
                              - "match_time TIMESTAMP"
                              - "match_id BIGINT"
                              - "player_id STRING"
                              - "score BIGINT"
                          merge:
                            kind: Ledger
                            primaryKey:
                              - match_id
                              - player_id
                        - kind: SetVocab
                          eventTimeColumn: match_time
                    "#
                ),
            )
            .unwrap();

            kamu_in_push_workspace
                .execute(["add", dataset_path.to_str().unwrap()])
                .await
                .unwrap();
        }

        // 2.1. Ingest data to the dataset
        {
            let dataset_data_path = kamu_in_push_workspace
                .workspace_path()
                .join("player-scores.data.ndjson");

            std::fs::write(
                dataset_data_path.clone(),
                indoc::indoc!(
                    r#"
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
                    "#,
                ),
            )
            .unwrap();

            kamu_in_push_workspace
                .execute([
                    "ingest",
                    "player-scores",
                    dataset_data_path.to_str().unwrap(),
                ])
                .await
                .unwrap();
        }

        // 2.2. Login to the API server
        kamu_in_push_workspace
            .execute([
                "login",
                kamu_api_server_client.get_base_url().as_str(),
                "--access-token",
                token.as_str(),
            ])
            .await
            .unwrap();

        // 2.3. Push the dataset to the API server
        kamu_in_push_workspace
            .execute([
                "push",
                "player-scores",
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
            ])
            .await
            .unwrap();
    }

    // 3. Pulling the dataset from the API server
    {
        let kamu_in_pull_workspace = Kamu::new_workspace_tmp().await;

        kamu_in_pull_workspace
            .execute(["pull", kamu_api_server_dataset_endpoint.as_str()])
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
