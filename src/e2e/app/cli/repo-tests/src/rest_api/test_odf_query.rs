// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::BTreeSet;

use chrono::{NaiveTime, SecondsFormat, Utc};
use http_common::comma_separated::CommaSeparatedSet;
use kamu_adapter_http::data::metadata_handler::{
    DatasetMetadataResponse,
    Include as MetadataInclude,
    Output as MetadataOutput,
    Ref as MetadataOutputRef,
};
use kamu_adapter_http::data::query_types::{Include as QueryInclude, QueryRequest};
use kamu_adapter_http::data::verify_types::{VerifyRequest, VerifyResponse};
use kamu_cli_e2e_common::{
    CreateDatasetResponse,
    KamuApiServerClient,
    KamuApiServerClientExt,
    RequestBody,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    E2E_USER_ACCOUNT_NAME,
};
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

pub async fn test_verify(mut kamu_api_server_client: KamuApiServerClient) {
    const QUERY: &str = indoc::indoc!(
        r#"
        SELECT player_id,
               score
        FROM 'player-scores'
        ORDER BY match_id, score, player_id
        "#
    );

    kamu_api_server_client.auth().login_as_kamu().await;

    kamu_api_server_client
        .dataset()
        .create_player_scores_dataset_with_data()
        .await;

    let query_request = QueryRequest {
        query: QUERY.into(),
        include: BTreeSet::from([QueryInclude::Proof]),
        ..Default::default()
    };
    let query_result = kamu_api_server_client
        .odf_query()
        .query_via_rest(&query_request)
        .await;

    let query_response = query_result.unwrap();

    assert_matches!(
        kamu_api_server_client
            .odf_query()
            .verify(VerifyRequest {
                input: query_response.input.unwrap(),
                sub_queries: query_response.sub_queries.unwrap(),
                commitment: query_response.commitment.unwrap(),
                proof: query_response.proof.unwrap(),
            })
            .await,
        Ok(VerifyResponse {
            ok: true,
            error: None
        })
    );
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
        json!({
            "data": [],
            "dataFormat": "JsonAoS",
        }),
        kamu_api_server_client
            .odf_query()
            .tail(&dataset_alias)
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
            .odf_query()
            .tail(&dataset_alias)
            .await
    );
}

async fn test_dataset_metadata(
    kamu_api_server_client: KamuApiServerClient,
    maybe_account_name: Option<odf::AccountName>,
) {
    let CreateDatasetResponse {
        dataset_id: expected_dataset_id,
        ..
    } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    let dataset_alias =
        odf::DatasetAlias::new(maybe_account_name, DATASET_ROOT_PLAYER_NAME.clone());
    let head_hash = kamu_api_server_client
        .odf_transfer()
        .metadata_block_hash_by_ref(&dataset_alias, odf::BlockRef::Head)
        .await
        .unwrap();
    let expected_refs = vec![MetadataOutputRef {
        name: odf::BlockRef::Head.as_str().into(),
        block_hash: head_hash,
    }];

    assert_matches!(
        kamu_api_server_client
            .odf_query()
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
                    odf::metadata::Seed {
                        dataset_kind: odf::DatasetKind::Root,
                        dataset_id
                    }
                ),
                vocab: Some(vocab),
            }
        })
            if vocab.offset_column == "offset"
                && vocab.operation_type_column == "op"
                && vocab.system_time_column == "system_time"
                && vocab.event_time_column == "match_time"
                && refs == expected_refs
                && dataset_id == expected_dataset_id
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
