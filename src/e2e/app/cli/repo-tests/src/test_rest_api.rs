// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{NaiveTime, SecondsFormat, Utc};
use kamu_cli_e2e_common::{
    ExpectedResponseBody,
    KamuApiServerClient,
    KamuApiServerClientExt,
    RequestBody,
};
use reqwest::{Method, StatusCode};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rest_api_request_dataset_tail(kamu_api_server_client: KamuApiServerClient) {
    // 1. Grub a token
    let token = kamu_api_server_client.login_as_kamu().await;

    // 2. Create a dataset
    kamu_api_server_client
        .create_player_scores_dataset(&token)
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
