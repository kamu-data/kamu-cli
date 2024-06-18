// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{KamuApiServerClient, RequestBody};
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
            Some(RequestBody::NDJSON(
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

    // 5. Get the dataset tail
    kamu_api_server_client
        .rest_api_call_assert(
            None,
            Method::GET,
            "player-scores/tail?limit=10",
            None,
            StatusCode::OK,
            Some(r#"{"data":[{"offset":0,"op":0,"system_time":"2024-06-18T15:00:00Z","match_time":"2000-01-01T00:00:00Z","match_id":1,"player_id":"Alice","score":100},{"offset":1,"op":0,"system_time":"2024-06-18T15:00:00Z","match_time":"2000-01-01T00:00:00Z","match_id":1,"player_id":"Bob","score":80}],"schema":"{\"fields\":[{\"name\":\"offset\",\"data_type\":\"Int64\",\"nullable\":true,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"op\",\"data_type\":\"Int32\",\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"system_time\",\"data_type\":{\"Timestamp\":[\"Millisecond\",\"UTC\"]},\"nullable\":false,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"match_time\",\"data_type\":{\"Timestamp\":[\"Millisecond\",\"UTC\"]},\"nullable\":true,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"match_id\",\"data_type\":\"Int64\",\"nullable\":true,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"player_id\",\"data_type\":\"Utf8\",\"nullable\":true,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}},{\"name\":\"score\",\"data_type\":\"Int64\",\"nullable\":true,\"dict_id\":0,\"dict_is_ordered\":false,\"metadata\":{}}],\"metadata\":{}}"}"#),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////
