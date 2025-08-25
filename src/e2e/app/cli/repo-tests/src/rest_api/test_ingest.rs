// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_cli_e2e_common::{
    CreateDatasetResponse,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    KamuApiServerClient,
    KamuApiServerClientExt,
    RequestBody,
};

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
                                        paused: false,
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
