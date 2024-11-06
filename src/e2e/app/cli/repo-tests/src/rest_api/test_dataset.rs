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
use kamu::domain::BlockRef;
use kamu_adapter_http::data::metadata_handler::{
    DatasetMetadataResponse,
    Include as MetadataInclude,
    Output as MetadataOutput,
    Ref as MetadataOutputRef,
};
use kamu_adapter_http::general::DatasetInfoResponse;
use kamu_cli_e2e_common::{
    CreateDatasetResponse,
    DatasetByIdError,
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

pub async fn test_datasets_by_id(mut kamu_api_server_client: KamuApiServerClient) {
    // TODO: Remove login after fixing
    //       https://github.com/kamu-data/kamu-cli/issues/730 -->
    let (_, nonexistent_dataset_id) = odf::DatasetID::new_generated_ed25519();

    assert_matches!(
        kamu_api_server_client
            .dataset()
            .by_id(&nonexistent_dataset_id)
            .await,
        Err(DatasetByIdError::Unauthorized)
    );

    kamu_api_server_client.auth().login_as_kamu().await;

    assert_matches!(
        kamu_api_server_client
            .dataset()
            .by_id(&nonexistent_dataset_id)
            .await,
        Err(DatasetByIdError::NotFound)
    );
    // <--

    let CreateDatasetResponse { dataset_id } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    // let expected_owner = DatasetOwnerInfo {
    //     account_name: odf::AccountName::new_unchecked("kamu"),
    //     // TODO: Private Datasets: replace with account id
    //     account_id: None,
    // };

    assert_matches!(
        kamu_api_server_client.dataset().by_id(&dataset_id).await,
        Ok(DatasetInfoResponse {
            id,
            owner,
            dataset_name
        })
            if id == dataset_id
                && owner.is_none() // TODO: Private Datasets: use expected_owner
                && dataset_name == odf::DatasetName::new_unchecked("player-scores")
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
    let head_hash = kamu_api_server_client
        .odf_transfer()
        .metadata_block_hash_by_ref(&dataset_alias, BlockRef::Head)
        .await
        .unwrap();
    let expected_refs = vec![MetadataOutputRef {
        name: BlockRef::Head.as_str().into(),
        block_hash: head_hash,
    }];

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
                && refs == expected_refs
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
