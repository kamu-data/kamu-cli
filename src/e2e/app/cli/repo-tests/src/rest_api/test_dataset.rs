// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::{NaiveTime, SecondsFormat, Utc};
use http_common::comma_separated::CommaSeparatedSet;
use kamu_adapter_http::data::metadata_handler::{
    DatasetMetadataResponse,
    Include as MetadataInclude,
    Output as MetadataOutput,
};
use kamu_cli_e2e_common::{
    KamuApiServerClient,
    KamuApiServerClientExt,
    RequestBody,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    E2E_USER_ACCOUNT_NAME,
};
use opendatafabric as odf;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_tail_st(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    test_dataset_tail(kamu_api_server_client, None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_tail_mt(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_e2e_user().await;

    test_dataset_tail(kamu_api_server_client, Some(E2E_USER_ACCOUNT_NAME.clone())).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_metadata_st(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    test_dataset_metadata(kamu_api_server_client, None).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_dataset_metadata_mt(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_e2e_user().await;

    test_dataset_metadata(kamu_api_server_client, Some(E2E_USER_ACCOUNT_NAME.clone())).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_tail(
    kamu_api_server_client: KamuApiServerClient,
    maybe_account_name: Option<odf::AccountName>,
) {
    // Create a dataset
    kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    // Try to get the dataset tail
    let dataset_alias =
        odf::DatasetAlias::new(maybe_account_name, DATASET_ROOT_PLAYER_NAME.clone());

    pretty_assertions::assert_eq!(
        serde_json::Value::Null,
        kamu_api_server_client
            .dataset()
            .tail_data_via_rest(&dataset_alias)
            .await
    );

    // Ingest data
    kamu_api_server_client
        .dataset()
        .ingest_data(
            &dataset_alias,
            RequestBody::NdJson(DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into()),
        )
        .await;

    let today = {
        let now = Utc::now();

        now.with_time(NaiveTime::from_hms_micro_opt(0, 0, 0, 0).unwrap())
            .unwrap()
            .to_rfc3339_opts(SecondsFormat::Secs, true)
    };

    // Get the dataset tail
    pretty_assertions::assert_eq!(
        json!({
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
        }),
        kamu_api_server_client
            .dataset()
            .tail_data_via_rest(&dataset_alias)
            .await
    );
}

async fn test_dataset_metadata(
    kamu_api_server_client: KamuApiServerClient,
    maybe_account_name: Option<odf::AccountName>,
) {
    kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    let dataset_alias =
        odf::DatasetAlias::new(maybe_account_name, DATASET_ROOT_PLAYER_NAME.clone());

    assert_matches!(
        kamu_api_server_client
            .dataset()
            .metadata(
                &dataset_alias,
                Some(CommaSeparatedSet::from([
                    MetadataInclude::Attachments,
                    MetadataInclude::Info,
                    MetadataInclude::License,
                    MetadataInclude::Refs,
                    MetadataInclude::Schema,
                    MetadataInclude::Seed,
                    MetadataInclude::Vocab,
                ])),
            )
            .await,
        Ok(DatasetMetadataResponse {
            output: MetadataOutput {
                attachments: None,
                info: None,
                license: None,
                refs: Some(refs),
                schema: None,
                schema_format: None,
                seed: Some(
                    odf::Seed {
                        dataset_kind: odf::DatasetKind::Root,
                        ..
                    }
                ),
                vocab: Some(vocab),
            }
        })
            if vocab.offset_column == "offset"
                && vocab.operation_type_column == "op"
                && vocab.system_time_column == "system_time"
                && vocab.event_time_column == "match_time"
                && refs.len() == 1
                && refs.iter().any(|r#ref| r#ref.name == "head")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
