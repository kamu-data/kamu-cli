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
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
};
use reqwest::{Method, StatusCode};
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rest_api_request_dataset_tail(mut kamu_api_server_client: KamuApiServerClient) {
    // 1. Grub a token
    kamu_api_server_client.auth().login_as_kamu().await;

    // 2. Create a dataset
    kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    // 3. Try to get the dataset tail
    kamu_api_server_client
        .rest_api_call_assert(
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
            Method::POST,
            "player-scores/ingest",
            Some(RequestBody::NdJson(
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into(),
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
            Method::GET,
            "player-scores/tail",
            None,
            StatusCode::OK,
            Some(ExpectedResponseBody::Json(json!({
                "data": [
                    {
                        "match_id": 1,
                        "match_time": "2000-01-01T00:00:00Z",
                        "offset": 0,
                        "op": 0,
                        "player_id": "Alice",
                        "score": 100,
                        "system_time": today.as_str(),
                    },
                    {
                        "match_id": 1,
                        "match_time": "2000-01-01T00:00:00Z",
                        "offset": 1,
                        "op": 0,
                        "player_id": "Bob",
                        "score": 80,
                        "system_time": today.as_str(),
                    }
                ],
                "dataFormat": "JsonAoS"
            }))),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
