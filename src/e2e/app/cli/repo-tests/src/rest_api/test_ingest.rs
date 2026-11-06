// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_cli_e2e_common::{
    CreateDatasetResponse,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    KamuApiServerClient,
    KamuApiServerClientExt,
    RequestBody,
};
use reqwest::{Method, StatusCode};

use crate::utils::generate_password;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MULTITENANT_QUOTA_INGEST_TEST_CONFIG: &str = indoc::indoc!(
    r#"
    kind: CLIConfig
    version: 1
    content:
      auth:
        allowAnonymous: false
        users:
          predefined:
            - accountName: kamu
              password: kamu.dev
              email: kamu@example.com
              properties: [ admin ]
            - accountName: quota-low
              password: test#quota-low
              email: quota-low@example.com
            - accountName: quota-ok
              password: test#quota-ok
              email: quota-ok@example.com
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_ingest_dataset_trigger_dependent_datasets_update(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse {
        dataset_alias: root_dataset_alias,
        ..
    } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    let CreateDatasetResponse {
        dataset_id: derivative_dataset_id,
        ..
    } = kamu_api_server_client.dataset().create_leaderboard().await;

    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                mutation {
                    datasets {
                        byId (datasetId: $datasetId) {
                            flows {
                                triggers {
                                    setTrigger (
                                        datasetFlowType: $datasetFlowType,
                                        triggerRuleInput: {
                                            reactive: {
                                                forNewData: {
                                                    immediate: { dummy: false }
                                                },
                                                forBreakingChange: "NO_ACTION"
                                            }
                                        },
                                        triggerStopPolicyInput: {
                                            afterConsecutiveFailures: { maxFailures: 1 }
                                        }
                                    ) {
                                        __typename,
                                        message
                                        ... on SetFlowTriggerSuccess {
                                            trigger {
                                                __typename
                                                reactive {
                                                    __typename
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            )
            .replace("$datasetFlowType", "\"EXECUTE_TRANSFORM\"")
            .replace("$datasetId", &format!("\"{derivative_dataset_id}\""))
            .as_str(),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "byId": {
                      "flows": {
                        "triggers": {
                          "setTrigger": {
                            "__typename": "SetFlowTriggerSuccess",
                            "message": "Success",
                            "trigger": {
                              "__typename": "FlowTrigger",
                              "reactive": {
                                "__typename": "FlowTriggerReactiveRule"
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                "#
            )),
        )
        .await;

    kamu_api_server_client
        .dataset()
        .ingest_data(
            &root_dataset_alias,
            RequestBody::NdJson(DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into()),
        )
        .await;

    let root_dataset_query = indoc::indoc!(
        r#"
        SELECT match_time,
               match_id,
               player_id,
               score
        FROM 'player-scores'
        ORDER BY match_id, player_id
        "#
    );

    assert_matches!(
        kamu_api_server_client.odf_query().query(root_dataset_query).await,
        Ok(result)
            if result == indoc::indoc!(
                r#"
                match_time,match_id,player_id,score
                2000-01-01T00:00:00Z,1,Alice,100
                2000-01-01T00:00:00Z,1,Bob,80"#
            )
    );

    kamu_api_server_client
        .flow()
        .wait(&derivative_dataset_id, 1)
        .await;

    let derivative_dataset_query = indoc::indoc!(
        r#"
        SELECT match_time,
               match_id,
               player_id,
               place,
               score
        FROM 'leaderboard'
        ORDER BY match_id, player_id
        "#
    );

    assert_matches!(
        kamu_api_server_client.odf_query().query(derivative_dataset_query).await,
        Ok(result)
            if result == indoc::indoc!(
                r#"
                match_time,match_id,player_id,place,score
                2000-01-01T00:00:00Z,1,Alice,1,100
                2000-01-01T00:00:00Z,1,Bob,2,80"#
            )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_ingest_respects_account_quota(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    let low_account = "quota-low";
    let ok_account = "quota-ok";

    kamu_api_server_client
        .account()
        .set_account_quota_bytes(low_account, 1)
        .await;
    kamu_api_server_client
        .account()
        .set_account_quota_bytes(ok_account, 1_000_000)
        .await;

    kamu_api_server_client.auth().logout();
    kamu_api_server_client
        .auth()
        .login_with_password(
            low_account,
            &generate_password(&odf::AccountName::new_unchecked(low_account)),
        )
        .await;

    let low_quota_dataset = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await
        .dataset_alias;

    let low_quota_response = kamu_api_server_client
        .rest_api_call(
            reqwest::Method::POST,
            &format!("{low_quota_dataset}/ingest"),
            Some(RequestBody::NdJson(
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into(),
            )),
        )
        .await;

    pretty_assertions::assert_eq!(StatusCode::FORBIDDEN, low_quota_response.status());

    kamu_api_server_client.auth().logout();
    kamu_api_server_client
        .auth()
        .login_with_password(
            ok_account,
            &generate_password(&odf::AccountName::new_unchecked(ok_account)),
        )
        .await;

    let ok_dataset = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await
        .dataset_alias;

    kamu_api_server_client
        .dataset()
        .ingest_data(
            &ok_dataset,
            RequestBody::NdJson(DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into()),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The push source auto-created for an upload is committed in the transaction
/// of the ingest, so a failed ingest does not keep it. A valid upload into a
/// dataset without a source shows that the source is created on this path
pub async fn test_ingest_upload_failure_keeps_no_auto_created_source(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    kamu_api_server_client.auth().login_as_kamu().await;

    let broken_dataset = kamu_api_server_client
        .dataset()
        .create_empty_dataset(
            odf::DatasetKind::Root,
            &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("broken-upload")),
            &odf::DatasetVisibility::Private,
        )
        .await;
    let valid_dataset = kamu_api_server_client
        .dataset()
        .create_empty_dataset(
            odf::DatasetKind::Root,
            &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("valid-upload")),
            &odf::DatasetVisibility::Private,
        )
        .await;

    let broken_upload = kamu_api_server_client
        .upload()
        .prepare_and_upload("broken.ndjson", "{ this is not json")
        .await;
    let valid_upload = kamu_api_server_client
        .upload()
        .prepare_and_upload(
            "valid.ndjson",
            DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
        )
        .await;

    pretty_assertions::assert_eq!(
        StatusCode::BAD_REQUEST,
        kamu_api_server_client
            .dataset()
            .ingest_upload(&broken_dataset.dataset_alias, &broken_upload)
            .await
    );
    pretty_assertions::assert_eq!(
        StatusCode::OK,
        kamu_api_server_client
            .dataset()
            .ingest_upload(&valid_dataset.dataset_alias, &valid_upload)
            .await
    );

    pretty_assertions::assert_eq!(
        vec![odf::metadata::MetadataEventTypeFlags::SEED],
        kamu_api_server_client
            .dataset()
            .blocks(&broken_dataset.dataset_id)
            .await
            .blocks
            .into_iter()
            .map(|block| block.event)
            .collect::<Vec<_>>()
    );
    assert!(
        kamu_api_server_client
            .dataset()
            .blocks(&valid_dataset.dataset_id)
            .await
            .blocks
            .into_iter()
            .any(|block| block.event == odf::metadata::MetadataEventTypeFlags::ADD_PUSH_SOURCE)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Concurrent ingests into one dataset race for its head: each either commits
/// its data or fails with a conflict, and none is lost or overwritten
pub async fn test_concurrent_ingests_commit_or_conflict(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    const CONCURRENT_INGESTS: usize = 8;

    kamu_api_server_client.auth().login_as_kamu().await;
    let CreateDatasetResponse {
        dataset_id,
        dataset_alias,
        ..
    } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    let endpoint = format!("{dataset_alias}/ingest");
    let responses = futures::future::join_all((0..CONCURRENT_INGESTS).map(|match_id| {
        let row = format!(
            r#"{{"match_time": "2000-01-01T00:00:00Z", "match_id": {match_id}, "player_id": "Alice", "score": 100}}"#
        );
        kamu_api_server_client.rest_api_call(
            Method::POST,
            &endpoint,
            Some(RequestBody::NdJson(row)),
        )
    }))
    .await;

    let mut outcomes = Vec::with_capacity(responses.len());
    for response in responses {
        let status = response.status();
        outcomes.push((status, response.text().await.unwrap()));
    }
    assert!(
        outcomes
            .iter()
            .all(|(status, _)| [StatusCode::OK, StatusCode::CONFLICT].contains(status)),
        "{outcomes:#?}"
    );
    let committed = outcomes
        .iter()
        .filter(|(status, _)| *status == StatusCode::OK)
        .count();

    let add_data_blocks = kamu_api_server_client
        .dataset()
        .blocks(&dataset_id)
        .await
        .blocks
        .into_iter()
        .filter(|block| block.event == odf::metadata::MetadataEventTypeFlags::ADD_DATA)
        .count();
    pretty_assertions::assert_eq!(committed, add_data_blocks, "{outcomes:#?}");
    pretty_assertions::assert_eq!(
        format!("num_records\n{committed}"),
        kamu_api_server_client
            .odf_query()
            .query("SELECT count(*) AS num_records FROM 'player-scores'")
            .await
            .unwrap(),
        "{outcomes:#?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
