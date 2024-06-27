// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{NaiveTime, SecondsFormat, Utc};
use kamu_cli_e2e_common::{ExpectedResponseBody, KamuApiServerClient, RequestBody};
use reqwest::{Method, StatusCode};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    // 2. Create a dataset
    let snapshot = indoc::indoc!(
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
    )
    .escape_default()
    .to_string();

    kamu_api_server_client
        .graphql_api_call_assert_with_token(
            token.clone(),
            indoc::indoc!(
                r#"
                mutation {
                  datasets {
                    createFromSnapshot(snapshot: "<snapshot>", snapshotFormat: YAML) {
                      message
                    }
                  }
                }
                "#,
            )
            .replace("<snapshot>", snapshot.as_str())
            .as_str(),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "createFromSnapshot": {
                      "message": "Success"
                    }
                  }
                }
                "#,
            )),
        )
        .await;

    // 3. Try to get the dataset tail
    kamu_api_server_client
        .rest_api_call_assert(
            None,
            Method::GET,
            "player-scores/tail?limit=10",
            None,
            StatusCode::NO_CONTENT,
            None,
        )
        .await;

    // 4. Ingest data
    kamu_api_server_client
        .rest_api_call_assert(
            Some(token),
            Method::POST,
            "player-scores/ingest",
            Some(RequestBody::NdJson(
                indoc::indoc!(
                    r#"
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
                    "#,
                )
                .into(),
            )),
            StatusCode::OK,
            None,
        )
        .await;

    let today = {
        let now = Utc::now();

        now.with_time(NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap())
            .unwrap()
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    };

    // 5. Get the dataset tail
    kamu_api_server_client
        .rest_api_call_assert(
            None,
            Method::GET,
            "player-scores/tail?includeSchema=false",
            None,
            StatusCode::OK,
            Some(ExpectedResponseBody::Json(
                indoc::indoc!(
                    r#"
                    {
                      "data": [
                        {
                          "match_id": 1,
                          "match_time": "2000-01-01T00:00:00Z",
                          "offset": 0,
                          "op": 0,
                          "player_id": "Alice",
                          "score": 100,
                          "system_time": "<SYSTEM_TIME>"
                        },
                        {
                          "match_id": 1,
                          "match_time": "2000-01-01T00:00:00Z",
                          "offset": 1,
                          "op": 0,
                          "player_id": "Bob",
                          "score": 80,
                          "system_time": "<SYSTEM_TIME>"
                        }
                      ]
                    }
                    "#
                )
                .to_string()
                .replace("<SYSTEM_TIME>", today.as_str()),
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
