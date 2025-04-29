// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::{TimeZone, Utc};
use kamu_cli_e2e_common::{
    CreateDatasetResponse,
    FlowTriggerResponse,
    KamuApiServerClient,
    KamuApiServerClientExt,
    RequestBody,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_3,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_flow_system::DatasetFlowType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_get_dataset_list_flows(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse { dataset_id, .. } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset_with_data()
        .await;

    // The query is almost identical to kamu-web-ui, for ease of later edits.
    // Except for commented dynamic dataset ID fields:
    // - search FlowDescriptionDatasetHardCompaction (datasetId)
    // - search DatasetBasics (id)

    kamu_api_server_client
        .graphql_api_call_assert(
            &get_dataset_list_flows_query(&dataset_id),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "__typename": "Datasets",
                    "byId": {
                      "__typename": "Dataset",
                      "alias": "player-scores",
                      "flows": {
                        "__typename": "DatasetFlows",
                        "runs": {
                          "__typename": "DatasetFlowRuns",
                          "table": {
                            "__typename": "FlowConnection",
                            "edges": [],
                            "nodes": [],
                            "pageInfo": {
                              "__typename": "PageBasedInfo",
                              "currentPage": 0,
                              "hasNextPage": false,
                              "hasPreviousPage": false,
                              "totalPages": 0
                            },
                            "totalCount": 0
                          },
                          "tiles": {
                            "__typename": "FlowConnection",
                            "nodes": [],
                            "totalCount": 0
                          }
                        }
                      },
                      "kind": "ROOT",
                      "metadata": {
                        "__typename": "DatasetMetadata",
                        "currentPollingSource": null,
                        "currentTransform": null
                      },
                      "name": "player-scores",
                      "owner": {
                        "__typename": "Account",
                        "accountName": "kamu",
                        "id": "did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f"
                      }
                    }
                  }
                }
                "#
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_dataset_all_flows_paused(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse { dataset_id, .. } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset_with_data()
        .await;

    // The query is almost identical to kamu-web-ui, for ease of later edits.

    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                query datasetAllFlowsPaused() {
                  datasets {
                    byId(datasetId: $datasetId) {
                      flows {
                        triggers {
                          allPaused
                          __typename
                        }
                        __typename
                      }

                      __typename
                    }
                    __typename
                  }
                }
                "#
            )
            .replace("$datasetId", &format!("\"{dataset_id}\""))
            .as_str(),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "__typename": "Datasets",
                    "byId": {
                      "__typename": "Dataset",
                      "flows": {
                        "__typename": "DatasetFlows",
                        "triggers": {
                          "__typename": "DatasetFlowTriggers",
                          "allPaused": true
                        }
                      }
                    }
                  }
                }
                "#
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_dataset_flows_initiators(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse { dataset_id, .. } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset_with_data()
        .await;

    // The query is almost identical to kamu-web-ui, for ease of later edits.

    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                query datasetFlowsInitiators() {
                  datasets {
                    byId(datasetId: $datasetId) {
                      flows {
                        runs {
                          listFlowInitiators {
                            totalCount
                            nodes {
                              ...Account
                              __typename
                            }
                            __typename
                          }
                          __typename
                        }
                        __typename
                      }

                      __typename
                    }
                    __typename
                  }
                }

                fragment Account on Account {
                  id
                  accountName
                  displayName
                  accountType
                  avatarUrl
                  isAdmin
                  __typename
                }
                "#
            )
            .replace("$datasetId", &format!("\"{dataset_id}\""))
            .as_str(),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "__typename": "Datasets",
                    "byId": {
                      "__typename": "Dataset",
                      "flows": {
                        "__typename": "DatasetFlows",
                        "runs": {
                          "__typename": "DatasetFlowRuns",
                          "listFlowInitiators": {
                            "__typename": "AccountConnection",
                            "nodes": [],
                            "totalCount": 0
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_dataset_trigger_flow(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    kamu_api_server_client
        .dataset()
        .create_player_scores_dataset_with_data()
        .await;

    let CreateDatasetResponse {
        dataset_id: derivative_dataset_id,
        ..
    } = kamu_api_server_client.dataset().create_leaderboard().await;

    // The query is almost identical to kamu-web-ui, for ease of later edits.
    // Except for commented dynamic dataset ID fields:
    // - search FlowDescriptionDatasetPollingIngest (datasetId)
    // - search FlowDescriptionDatasetPushIngest (datasetId)
    // - search FlowDescriptionDatasetExecuteTransform (datasetId)
    // - search FlowDescriptionDatasetHardCompaction (datasetId)
    // - search FlowSummaryData (datasetId, flowId)

    kamu_api_server_client
        .graphql_api_call_assert(
            indoc::indoc!(
                r#"
                mutation datasetTriggerFlow() {
                  datasets {
                    byId(datasetId: $datasetId) {
                      flows {
                        runs {
                          triggerFlow(
                            datasetFlowType: $datasetFlowType
                            flowRunConfiguration: $flowRunConfiguration
                          ) {
                            ... on TriggerFlowSuccess {
                              flow {
                                ...FlowSummaryData
                                __typename
                              }
                              message
                              __typename
                            }
                            ... on FlowIncompatibleDatasetKind {
                              expectedDatasetKind
                              actualDatasetKind
                              message
                              __typename
                            }
                            ... on FlowPreconditionsNotMet {
                              message
                              __typename
                            }
                            ... on FlowInvalidRunConfigurations {
                              error
                              message
                              __typename
                            }
                            __typename
                          }
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                }

                fragment FlowSummaryData on Flow {
                  description {
                    ... on FlowDescriptionDatasetPollingIngest {
                      # datasetId
                      ingestResult {
                        ... on FlowDescriptionUpdateResultUpToDate {
                          uncacheable
                          __typename
                        }
                        ... on FlowDescriptionUpdateResultSuccess {
                          numBlocks
                          numRecords
                          updatedWatermark
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    ... on FlowDescriptionDatasetPushIngest {
                      # datasetId
                      sourceName
                      inputRecordsCount
                      ingestResult {
                        ... on FlowDescriptionUpdateResultUpToDate {
                          uncacheable
                          __typename
                        }
                        ... on FlowDescriptionUpdateResultSuccess {
                          numBlocks
                          numRecords
                          updatedWatermark
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    ... on FlowDescriptionDatasetExecuteTransform {
                      # datasetId
                      transformResult {
                        ... on FlowDescriptionUpdateResultUpToDate {
                          uncacheable
                          __typename
                        }
                        ... on FlowDescriptionUpdateResultSuccess {
                          numBlocks
                          numRecords
                          updatedWatermark
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    ... on FlowDescriptionDatasetHardCompaction {
                      # datasetId
                      compactionResult {
                        ... on FlowDescriptionHardCompactionSuccess {
                          originalBlocksCount
                          resultingBlocksCount
                          newHead
                          __typename
                        }
                        ... on FlowDescriptionHardCompactionNothingToDo {
                          message
                          dummy
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    ... on FlowDescriptionSystemGC {
                      dummy
                      __typename
                    }
                    ... on FlowDescriptionDatasetReset {
                      datasetId
                      resetResult {
                        newHead
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  # flowId
                  status
                  initiator {
                    ...Account
                    __typename
                  }
                  outcome {
                    ...FlowOutcomeData
                    __typename
                  }
                  timing {
                    awaitingExecutorSince
                    runningSince
                    finishedAt
                    __typename
                  }
                  startCondition {
                    __typename
                    ... on FlowStartConditionThrottling {
                      intervalSec
                      wakeUpAt
                      shiftedFrom
                      __typename
                    }
                    ... on FlowStartConditionBatching {
                      activeBatchingRule {
                        minRecordsToAwait
                        maxBatchingInterval {
                          ...TimeDeltaData
                          __typename
                        }
                        __typename
                      }
                      batchingDeadline
                      accumulatedRecordsCount
                      watermarkModified
                      __typename
                    }
                    ... on FlowStartConditionSchedule {
                      wakeUpAt
                      __typename
                    }
                    ... on FlowStartConditionExecutor {
                      taskId
                      __typename
                    }
                  }
                  configSnapshot {
                    ... on FlowConfigurationIngest {
                      fetchUncacheable
                      __typename
                    }
                    ... on FlowConfigurationCompactionRule {
                      compactionRule {
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  __typename
                }

                fragment Account on Account {
                  id
                  accountName
                  displayName
                  accountType
                  avatarUrl
                  isAdmin
                  __typename
                }

                fragment FlowOutcomeData on FlowOutcome {
                  ... on FlowSuccessResult {
                    message
                    __typename
                  }
                  ... on FlowFailedError {
                    reason {
                      ... on FlowFailureReasonGeneral {
                        message
                        __typename
                      }
                      ... on FlowFailureReasonInputDatasetCompacted {
                        message
                        inputDataset {
                          ...DatasetBasics
                          __typename
                        }
                        __typename
                      }
                      __typename
                    }
                    __typename
                  }
                  ... on FlowAbortedResult {
                    message
                    __typename
                  }
                  __typename
                }

                fragment DatasetBasics on Dataset {
                  id
                  kind
                  name
                  owner {
                    ...AccountBasics
                    __typename
                  }
                  alias
                  __typename
                }

                fragment AccountBasics on Account {
                  id
                  accountName
                  __typename
                }

                fragment TimeDeltaData on TimeDelta {
                  every
                  unit
                  __typename
                }
                "#
            )
                .replace(
                    "$accountId",
                    "\"did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f\"",
                )
                .replace("$datasetFlowType", "\"EXECUTE_TRANSFORM\"")
                .replace(
                    "$datasetId",
                    &format!("\"{derivative_dataset_id}\""),
                )
                .replace("$flowRunConfiguration", "null")
                .as_str(),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "__typename": "DatasetsMut",
                    "byId": {
                      "__typename": "DatasetMut",
                      "flows": {
                        "__typename": "DatasetFlowsMut",
                        "runs": {
                          "__typename": "DatasetFlowRunsMut",
                          "triggerFlow": {
                            "__typename": "TriggerFlowSuccess",
                            "flow": {
                              "__typename": "Flow",
                              "configSnapshot": null,
                              "description": {
                                "__typename": "FlowDescriptionDatasetExecuteTransform",
                                "transformResult": null
                              },
                              "initiator": {
                                "__typename": "Account",
                                "accountName": "kamu",
                                "accountType": "USER",
                                "avatarUrl": "https://avatars.githubusercontent.com/u/50896974?s=200&v=4",
                                "displayName": "kamu",
                                "id": "did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f",
                                "isAdmin": true
                              },
                              "outcome": null,
                              "startCondition": null,
                              "status": "WAITING",
                              "timing": {
                                "__typename": "FlowTimingRecords",
                                "awaitingExecutorSince": null,
                                "finishedAt": null,
                                "runningSince": null
                              }
                            },
                            "message": "Success"
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
        .flow()
        .wait(&derivative_dataset_id, 1)
        .await;

    kamu_api_server_client
        .graphql_api_call_assert(
            &get_dataset_list_flows_query(&derivative_dataset_id),
            Ok(indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "__typename": "Datasets",
                    "byId": {
                      "__typename": "Dataset",
                      "alias": "leaderboard",
                      "flows": {
                        "__typename": "DatasetFlows",
                        "runs": {
                          "__typename": "DatasetFlowRuns",
                          "table": {
                            "__typename": "FlowConnection",
                            "edges": [
                              {
                                "__typename": "FlowEdge",
                                "node": {
                                  "__typename": "Flow",
                                  "configSnapshot": null,
                                  "description": {
                                    "__typename": "FlowDescriptionDatasetExecuteTransform",
                                    "transformResult": {
                                      "__typename": "FlowDescriptionUpdateResultSuccess",
                                      "numBlocks": 2,
                                      "numRecords": 2,
                                      "updatedWatermark": "2000-01-01T00:00:00+00:00"
                                    }
                                  },
                                  "initiator": {
                                    "__typename": "Account",
                                    "accountName": "kamu",
                                    "accountType": "USER",
                                    "avatarUrl": "https://avatars.githubusercontent.com/u/50896974?s=200&v=4",
                                    "displayName": "kamu",
                                    "id": "did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f",
                                    "isAdmin": true
                                  },
                                  "outcome": {
                                    "__typename": "FlowSuccessResult",
                                    "message": "SUCCESS"
                                  },
                                  "startCondition": null,
                                  "status": "FINISHED",
                                  "timing": {
                                    "__typename": "FlowTimingRecords"
                                  }
                                }
                              }
                            ],
                            "nodes": [
                              {
                                "__typename": "Flow",
                                "configSnapshot": null,
                                "description": {
                                  "__typename": "FlowDescriptionDatasetExecuteTransform",
                                  "transformResult": {
                                    "__typename": "FlowDescriptionUpdateResultSuccess",
                                    "numBlocks": 2,
                                    "numRecords": 2,
                                    "updatedWatermark": "2000-01-01T00:00:00+00:00"
                                  }
                                },
                                "initiator": {
                                  "__typename": "Account",
                                  "accountName": "kamu",
                                  "accountType": "USER",
                                  "avatarUrl": "https://avatars.githubusercontent.com/u/50896974?s=200&v=4",
                                  "displayName": "kamu",
                                  "id": "did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f",
                                  "isAdmin": true
                                },
                                "outcome": {
                                  "__typename": "FlowSuccessResult",
                                  "message": "SUCCESS"
                                },
                                "startCondition": null,
                                "status": "FINISHED",
                                "timing": {
                                  "__typename": "FlowTimingRecords"
                                }
                              }
                            ],
                            "pageInfo": {
                              "__typename": "PageBasedInfo",
                              "currentPage": 0,
                              "hasNextPage": false,
                              "hasPreviousPage": false,
                              "totalPages": 1
                            },
                            "totalCount": 1
                          },
                          "tiles": {
                            "__typename": "FlowConnection",
                            "nodes": [
                              {
                                "__typename": "Flow",
                                "initiator": {
                                  "__typename": "Account",
                                  "accountName": "kamu"
                                },
                                "outcome": {
                                  "__typename": "FlowSuccessResult",
                                  "message": "SUCCESS"
                                },
                                "status": "FINISHED",
                                "timing": {
                                  "__typename": "FlowTimingRecords"
                                }
                              }
                            ],
                            "totalCount": 1
                          }
                        }
                      },
                      "kind": "DERIVATIVE",
                      "metadata": {
                        "__typename": "DatasetMetadata",
                        "currentPollingSource": null,
                        "currentTransform": {
                          "__typename": "SetTransform",
                          "inputs": [
                            {
                              "__typename": "TransformInput"
                            }
                          ],
                          "transform": {
                            "__typename": "TransformSql",
                            "engine": "datafusion"
                          }
                        }
                      },
                      "name": "leaderboard",
                      "owner": {
                        "__typename": "Account",
                        "accountName": "kamu",
                        "id": "did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f"
                      }
                    }
                  }
                }
                "#
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_trigger_flow_ingest(mut kamu_api_server_client: KamuApiServerClient) {
    const QUERY: &str = indoc::indoc!(
        r#"
        SELECT op,
               system_time,
               event_time,
               city,
               population
        FROM 'root-dataset'
        ORDER BY event_time, city, population
        "#
    );

    let temp_dir = tempfile::tempdir().unwrap();

    let root_dataset_snapshot = indoc::formatdoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: root-dataset
          kind: Root
          metadata:
            - kind: SetPollingSource
              fetch:
                kind: FilesGlob
                path: {}
              read:
                kind: Csv
                header: true
                schema:
                  - event_time TIMESTAMP
                  - city STRING
                  - population BIGINT
              merge:
                kind: Ledger
                primaryKey:
                  - event_time
                  - city
        "#,
        temp_dir.path().join("chunk-*.csv").display()
    )
    .escape_default()
    .to_string();

    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse {
        dataset_id: root_dataset_id,
        ..
    } = kamu_api_server_client
        .dataset()
        .create_dataset(&root_dataset_snapshot)
        .await;

    // No data before update

    pretty_assertions::assert_eq!(
        "",
        kamu_api_server_client
            .dataset()
            .tail_data(&root_dataset_id)
            .await
    );

    // Update iteration 1

    std::fs::write(
        temp_dir.path().join("chunk-1.csv"),
        indoc::indoc!(
            r#"
            event_time,city,population
            2020-01-01,A,1000
            2020-01-01,B,2000
            2020-01-01,C,3000
            "#
        ),
    )
    .unwrap();

    assert_matches!(
        kamu_api_server_client
            .flow()
            .trigger(&root_dataset_id, DatasetFlowType::Ingest)
            .await,
        FlowTriggerResponse::Success(_)
    );

    kamu_api_server_client
        .flow()
        .wait(&root_dataset_id, 1)
        .await;

    assert_matches!(
        kamu_api_server_client.odf_query().query(QUERY).await,
        Ok(result)
            if result == indoc::indoc!(
                r#"
                op,system_time,event_time,city,population
                0,2050-01-02T03:04:05Z,2020-01-01T00:00:00Z,A,1000
                0,2050-01-02T03:04:05Z,2020-01-01T00:00:00Z,B,2000
                0,2050-01-02T03:04:05Z,2020-01-01T00:00:00Z,C,3000"#
            )
    );

    // Update iteration 2

    let t = Utc.with_ymd_and_hms(2051, 1, 2, 3, 4, 5).unwrap();

    kamu_api_server_client.e2e().set_system_time(t).await;

    std::fs::write(
        temp_dir.path().join("chunk-2.csv"),
        indoc::indoc!(
            r#"
            event_time,city,population
            2020-01-02,A,1500
            2020-01-02,B,2500
            2020-01-02,C,3500
            "#
        ),
    )
    .unwrap();

    assert_matches!(
        kamu_api_server_client
            .flow()
            .trigger(&root_dataset_id, DatasetFlowType::Ingest)
            .await,
        FlowTriggerResponse::Success(_)
    );

    kamu_api_server_client
        .flow()
        .wait(&root_dataset_id, 2)
        .await;

    assert_matches!(
        kamu_api_server_client.odf_query().query(QUERY).await,
        Ok(result)
            if result == indoc::indoc!(
                r#"
                op,system_time,event_time,city,population
                0,2050-01-02T03:04:05Z,2020-01-01T00:00:00Z,A,1000
                0,2050-01-02T03:04:05Z,2020-01-01T00:00:00Z,B,2000
                0,2050-01-02T03:04:05Z,2020-01-01T00:00:00Z,C,3000
                0,2051-01-02T03:04:05Z,2020-01-02T00:00:00Z,A,1500
                0,2051-01-02T03:04:05Z,2020-01-02T00:00:00Z,B,2500
                0,2051-01-02T03:04:05Z,2020-01-02T00:00:00Z,C,3500"#
            )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_trigger_flow_ingest_no_polling_source(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    let root_dataset_snapshot = indoc::indoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: root-dataset
          kind: Root
          metadata: []
        "#
    )
    .escape_default()
    .to_string();

    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse {
        dataset_id: root_dataset_id,
        ..
    } = kamu_api_server_client
        .dataset()
        .create_dataset(&root_dataset_snapshot)
        .await;

    assert_matches!(
        kamu_api_server_client
            .flow()
            .trigger(&root_dataset_id, DatasetFlowType::Ingest)
            .await,
        FlowTriggerResponse::Error(message)
            if message == "Flow didn't met preconditions: 'No SetPollingSource event defined'"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_trigger_flow_execute_transform(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    kamu_api_server_client
        .dataset()
        .create_player_scores_dataset_with_data()
        .await;
    let CreateDatasetResponse {
        dataset_id: derivative_dataset_id,
        ..
    } = kamu_api_server_client.dataset().create_leaderboard().await;

    pretty_assertions::assert_eq!(
        "",
        kamu_api_server_client
            .dataset()
            .tail_data(&derivative_dataset_id)
            .await
    );

    assert_matches!(
        kamu_api_server_client
            .flow()
            .trigger(&derivative_dataset_id, DatasetFlowType::ExecuteTransform)
            .await,
        FlowTriggerResponse::Success(_)
    );

    kamu_api_server_client
        .flow()
        .wait(&derivative_dataset_id, 1)
        .await;

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            offset,op,system_time,match_time,place,match_id,player_id,score
            0,0,2050-01-02T03:04:05Z,2000-01-01T00:00:00Z,1,1,Alice,100
            1,0,2050-01-02T03:04:05Z,2000-01-01T00:00:00Z,2,1,Bob,80"#
        ),
        kamu_api_server_client
            .dataset()
            .tail_data(&derivative_dataset_id)
            .await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_trigger_flow_execute_transform_no_set_transform(
    mut kamu_api_server_client: KamuApiServerClient,
) {
    let derivative_dataset_snapshot = indoc::indoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: derivative-dataset
          kind: Derivative
          metadata: []
        "#
    )
    .escape_default()
    .to_string();

    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse {
        dataset_id: derivative_dataset_id,
        ..
    } = kamu_api_server_client
        .dataset()
        .create_dataset(&derivative_dataset_snapshot)
        .await;

    assert_matches!(
        kamu_api_server_client
            .flow()
            .trigger(&derivative_dataset_id, DatasetFlowType::ExecuteTransform)
            .await,
        FlowTriggerResponse::Error(message)
            if message == "Flow didn't met preconditions: 'No SetTransform event defined'"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_trigger_flow_hard_compaction(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse {
        dataset_id: root_dataset_id,
        ..
    } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;
    let dataset_alias = odf::DatasetAlias::new(None, DATASET_ROOT_PLAYER_NAME.clone());

    // Ingesting data in multiple chunks

    for chunk in [
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_3,
    ] {
        kamu_api_server_client
            .dataset()
            .ingest_data(&dataset_alias, RequestBody::NdJson(chunk.into()))
            .await;
    }

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            op,system_time,match_time,match_id,player_id,score
            0,2050-01-02T03:04:05Z,2000-01-01T00:00:00Z,1,Bob,80
            0,2050-01-02T03:04:05Z,2000-01-01T00:00:00Z,1,Alice,100
            0,2050-01-02T03:04:05Z,2000-01-02T00:00:00Z,2,Alice,70
            0,2050-01-02T03:04:05Z,2000-01-02T00:00:00Z,2,Charlie,90
            0,2050-01-02T03:04:05Z,2000-01-03T00:00:00Z,3,Bob,60
            0,2050-01-02T03:04:05Z,2000-01-03T00:00:00Z,3,Charlie,110"#
        ),
        kamu_api_server_client
            .odf_query()
            .query_player_scores_dataset()
            .await
    );

    // Verify that there are multiple date blocks (3 AddData)

    pretty_assertions::assert_eq!(
        vec![
            (6, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (5, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (4, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
            (2, odf::metadata::MetadataEventTypeFlags::SET_VOCAB),
            (1, odf::metadata::MetadataEventTypeFlags::ADD_PUSH_SOURCE),
            (0, odf::metadata::MetadataEventTypeFlags::SEED),
        ],
        kamu_api_server_client
            .dataset()
            .blocks(&root_dataset_id)
            .await
            .blocks
            .into_iter()
            .map(|block| (block.sequence_number, block.event))
            .collect::<Vec<_>>()
    );

    assert_matches!(
        kamu_api_server_client
            .flow()
            .trigger(&root_dataset_id, DatasetFlowType::HardCompaction)
            .await,
        FlowTriggerResponse::Success(_)
    );

    kamu_api_server_client
        .flow()
        .wait(&root_dataset_id, 1)
        .await;

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            op,system_time,match_time,match_id,player_id,score
            0,2050-01-02T03:04:05Z,2000-01-01T00:00:00Z,1,Bob,80
            0,2050-01-02T03:04:05Z,2000-01-01T00:00:00Z,1,Alice,100
            0,2050-01-02T03:04:05Z,2000-01-02T00:00:00Z,2,Alice,70
            0,2050-01-02T03:04:05Z,2000-01-02T00:00:00Z,2,Charlie,90
            0,2050-01-02T03:04:05Z,2000-01-03T00:00:00Z,3,Bob,60
            0,2050-01-02T03:04:05Z,2000-01-03T00:00:00Z,3,Charlie,110"#
        ),
        kamu_api_server_client
            .odf_query()
            .query_player_scores_dataset()
            .await
    );

    // Checking that there is now only one block of data

    pretty_assertions::assert_eq!(
        vec![
            (4, odf::metadata::MetadataEventTypeFlags::ADD_DATA),
            (3, odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA),
            (2, odf::metadata::MetadataEventTypeFlags::SET_VOCAB),
            (1, odf::metadata::MetadataEventTypeFlags::ADD_PUSH_SOURCE),
            (0, odf::metadata::MetadataEventTypeFlags::SEED),
        ],
        kamu_api_server_client
            .dataset()
            .blocks(&root_dataset_id)
            .await
            .blocks
            .into_iter()
            .map(|block| (block.sequence_number, block.event))
            .collect::<Vec<_>>()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_trigger_flow_reset(mut kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse {
        dataset_id: root_dataset_id,
        ..
    } = kamu_api_server_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    pretty_assertions::assert_eq!(
        vec![
            (2, odf::metadata::MetadataEventTypeFlags::SET_VOCAB),
            (1, odf::metadata::MetadataEventTypeFlags::ADD_PUSH_SOURCE),
            (0, odf::metadata::MetadataEventTypeFlags::SEED),
        ],
        kamu_api_server_client
            .dataset()
            .blocks(&root_dataset_id)
            .await
            .blocks
            .into_iter()
            .map(|block| (block.sequence_number, block.event))
            .collect::<Vec<_>>()
    );

    assert_matches!(
        kamu_api_server_client
            .flow()
            .trigger(&root_dataset_id, DatasetFlowType::Reset)
            .await,
        FlowTriggerResponse::Success(_)
    );

    kamu_api_server_client
        .flow()
        .wait(&root_dataset_id, 1)
        .await;

    pretty_assertions::assert_eq!(
        vec![(0, odf::metadata::MetadataEventTypeFlags::SEED)],
        kamu_api_server_client
            .dataset()
            .blocks(&root_dataset_id)
            .await
            .blocks
            .into_iter()
            .map(|block| (block.sequence_number, block.event))
            .collect::<Vec<_>>()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_flow_planing_failure(mut kamu_api_server_client: KamuApiServerClient) {
    let temp_dir = tempfile::tempdir().unwrap();

    let root_dataset_snapshot = indoc::formatdoc!(
        r#"
    kind: DatasetSnapshot
    version: 1
    content:
      name: root-dataset
      kind: Root
      metadata:
        - kind: SetPollingSource
          fetch:
            kind: FilesGlob
            path: {}
          read:
            kind: Csv
            header: true
            schema:
              - event_time TIMESTAMP
              - city STRING
              - population BIGINT
          merge:
            kind: Ledger
            primaryKey:
              - event_time
              - city
    "#,
        temp_dir.path().join("chunk-*.csv").display()
    )
    .escape_default()
    .to_string();

    kamu_api_server_client.auth().login_as_kamu().await;

    let CreateDatasetResponse {
        dataset_id,
        dataset_alias,
    } = kamu_api_server_client
        .dataset()
        .create_dataset(&root_dataset_snapshot)
        .await;

    let kamu = KamuCliPuppet::new(kamu_api_server_client.get_workspace_path());
    kamu.execute([
        "repo",
        "alias",
        "add",
        &dataset_alias.dataset_name,
        "http://foo",
        "--pull",
    ])
    .await
    .success();

    assert_matches!(
        kamu_api_server_client
            .flow()
            .trigger(&dataset_id, DatasetFlowType::Ingest)
            .await,
        FlowTriggerResponse::Success(_)
    );

    kamu_api_server_client.flow().wait(&dataset_id, 1).await;

    kamu_api_server_client
        .graphql_api_call_assert(
            &get_dataset_list_flows_query(&dataset_id),
            Ok(&indoc::indoc!(
                r#"
                {
                  "datasets": {
                    "__typename": "Datasets",
                    "byId": {
                      "__typename": "Dataset",
                      "alias": "root-dataset",
                      "flows": {
                        "__typename": "DatasetFlows",
                        "runs": {
                          "__typename": "DatasetFlowRuns",
                          "table": {
                            "__typename": "FlowConnection",
                            "edges": [
                              {
                                "__typename": "FlowEdge",
                                "node": {
                                  "__typename": "Flow",
                                  "configSnapshot": null,
                                  "description": {
                                    "__typename": "FlowDescriptionDatasetPollingIngest",
                                    "datasetId": $datasetId,
                                    "ingestResult": null
                                  },
                                  "initiator": {
                                    "__typename": "Account",
                                    "accountName": "kamu",
                                    "accountType": "USER",
                                    "avatarUrl": "https://avatars.githubusercontent.com/u/50896974?s=200&v=4",
                                    "displayName": "kamu",
                                    "id": "did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f",
                                    "isAdmin": true
                                  },
                                  "outcome": {
                                    "__typename": "FlowFailedError",
                                    "reason": {
                                      "__typename": "FlowFailureReasonGeneral",
                                      "message": "FAILED"
                                    }
                                  },
                                  "startCondition": null,
                                  "status": "FINISHED",
                                  "timing": {
                                    "__typename": "FlowTimingRecords"
                                  }
                                }
                              }
                            ],
                            "nodes": [
                              {
                                "__typename": "Flow",
                                "configSnapshot": null,
                                "description": {
                                  "__typename": "FlowDescriptionDatasetPollingIngest",
                                  "datasetId": $datasetId,
                                  "ingestResult": null
                                },
                                "initiator": {
                                  "__typename": "Account",
                                  "accountName": "kamu",
                                  "accountType": "USER",
                                  "avatarUrl": "https://avatars.githubusercontent.com/u/50896974?s=200&v=4",
                                  "displayName": "kamu",
                                  "id": "did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f",
                                  "isAdmin": true
                                },
                                "outcome": {
                                  "__typename": "FlowFailedError",
                                  "reason": {
                                    "__typename": "FlowFailureReasonGeneral",
                                    "message": "FAILED"
                                  }
                                },
                                "startCondition": null,
                                "status": "FINISHED",
                                "timing": {
                                  "__typename": "FlowTimingRecords"
                                }
                              }
                            ],
                            "pageInfo": {
                              "__typename": "PageBasedInfo",
                              "currentPage": 0,
                              "hasNextPage": false,
                              "hasPreviousPage": false,
                              "totalPages": 1
                            },
                            "totalCount": 1
                          },
                          "tiles": {
                            "__typename": "FlowConnection",
                            "nodes": [
                              {
                                "__typename": "Flow",
                                "initiator": {
                                  "__typename": "Account",
                                  "accountName": "kamu"
                                },
                                "outcome": {
                                  "__typename": "FlowFailedError",
                                  "reason": {
                                    "__typename": "FlowFailureReasonGeneral",
                                    "message": "FAILED"
                                  }
                                },
                                "status": "FINISHED",
                                "timing": {
                                  "__typename": "FlowTimingRecords"
                                }
                              }
                            ],
                            "totalCount": 1
                          }
                        }
                      },
                      "kind": "ROOT",
                      "metadata": {
                        "__typename": "DatasetMetadata",
                        "currentPollingSource": {
                          "__typename": "SetPollingSource",
                          "fetch": {
                            "__typename": "FetchStepFilesGlob",
                            "cache": null,
                            "eventTime": null,
                            "order": null,
                            "path": $tmpPath
                          }
                        },
                        "currentTransform": null
                      },
                      "name": "root-dataset",
                      "owner": {
                        "__typename": "Account",
                        "accountName": "kamu",
                        "id": "did:odf:fed016b61ed2ab1b63a006b61ed2ab1b63a00b016d65607000000e0821aafbf163e6f"
                      }
                    }
                  }
                }
                "#
            ).replace("$datasetId", &format!("\"{dataset_id}\""))
            .replace("$tmpPath", &format!("\"{}\"", temp_dir.path().join("chunk-*.csv").display()))
          ),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_dataset_list_flows_query(dataset_id: &odf::DatasetID) -> String {
    // The query is almost identical to kamu-web-ui, for ease of later edits.
    // Except for commented dynamic dataset ID fields:
    // - search FlowDescriptionDatasetHardCompaction (datasetId)
    // - search DatasetBasics (id)
    // - search FlowDescriptionDatasetExecuteTransform (id)
    // - search FlowItemWidgetData.timing:
    //   - awaitingExecutorSince
    //   - runningSince
    //   - finishedAt
    // - search FlowSummaryData (flowId)

    indoc::indoc!(
        r#"
        query getDatasetListFlows() {
          datasets {
            byId(datasetId: $datasetId) {
              ...DatasetListFlowsData
              flows {
                runs {
                  table: listFlows(
                    page: $page
                    perPage: $perPageTable
                    filters: $filters
                  ) {
                    ...FlowConnectionData
                    __typename
                  }
                  tiles: listFlows(
                    page: 0
                    perPage: $perPageTiles
                    filters: { byFlowType: null, byStatus: null, byInitiator: null }
                  ) {
                    ...FlowConnectionWidgetData
                    __typename
                  }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }

        fragment DatasetListFlowsData on Dataset {
          ...DatasetBasics
          metadata {
            currentPollingSource {
              fetch {
                ...FetchStepUrlData
                ...FetchStepFilesGlobData
                ...FetchStepContainerData
                __typename
              }
              __typename
            }
            currentTransform {
              inputs {
                __typename
              }
              transform {
                ... on TransformSql {
                  engine
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }

        fragment DatasetBasics on Dataset {
          # id
          kind
          name
          owner {
            ...AccountBasics
            __typename
          }
          alias
          __typename
        }

        fragment AccountBasics on Account {
          id
          accountName
          __typename
        }

        fragment FetchStepUrlData on FetchStepUrl {
          url
          eventTime {
            ... on EventTimeSourceFromPath {
              pattern
              timestampFormat
              __typename
            }
            ... on EventTimeSourceFromMetadata {
              __typename
            }
            ... on EventTimeSourceFromSystemTime {
              __typename
            }
            __typename
          }
          headers {
            name
            value
            __typename
          }
          cache {
            __typename
          }
          __typename
        }

        fragment FetchStepFilesGlobData on FetchStepFilesGlob {
          path
          eventTime {
            ... on EventTimeSourceFromPath {
              pattern
              timestampFormat
              __typename
            }
            ... on EventTimeSourceFromMetadata {
              __typename
            }
            ... on EventTimeSourceFromSystemTime {
              __typename
            }
            __typename
          }
          cache {
            __typename
          }
          order
          __typename
        }

        fragment FetchStepContainerData on FetchStepContainer {
          image
          command
          args
          env {
            name
            value
            __typename
          }
          __typename
        }

        fragment FlowConnectionData on FlowConnection {
          nodes {
            ...FlowSummaryData
            __typename
          }
          totalCount
          pageInfo {
            ...DatasetPageInfo
            __typename
          }
          edges {
            node {
              ...FlowSummaryData
              __typename
            }
            __typename
          }
          __typename
        }

        fragment FlowSummaryData on Flow {
          description {
            ... on FlowDescriptionDatasetPollingIngest {
              datasetId
              ingestResult {
                ... on FlowDescriptionUpdateResultUpToDate {
                  uncacheable
                  __typename
                }
                ... on FlowDescriptionUpdateResultSuccess {
                  numBlocks
                  numRecords
                  updatedWatermark
                  __typename
                }
                __typename
              }
              __typename
            }
            ... on FlowDescriptionDatasetPushIngest {
              datasetId
              sourceName
              inputRecordsCount
              ingestResult {
                ... on FlowDescriptionUpdateResultUpToDate {
                  uncacheable
                  __typename
                }
                ... on FlowDescriptionUpdateResultSuccess {
                  numBlocks
                  numRecords
                  updatedWatermark
                  __typename
                }
                __typename
              }
              __typename
            }
            ... on FlowDescriptionDatasetExecuteTransform {
              # datasetId
              transformResult {
                ... on FlowDescriptionUpdateResultUpToDate {
                  uncacheable
                  __typename
                }
                ... on FlowDescriptionUpdateResultSuccess {
                  numBlocks
                  numRecords
                  updatedWatermark
                  __typename
                }
                __typename
              }
              __typename
            }
            ... on FlowDescriptionDatasetHardCompaction {
              # datasetId
              compactionResult {
                ... on FlowDescriptionHardCompactionSuccess {
                  originalBlocksCount
                  resultingBlocksCount
                  newHead
                  __typename
                }
                ... on FlowDescriptionHardCompactionNothingToDo {
                  message
                  dummy
                  __typename
                }
                __typename
              }
              __typename
            }
            ... on FlowDescriptionSystemGC {
              dummy
              __typename
            }
            ... on FlowDescriptionDatasetReset {
              datasetId
              resetResult {
                newHead
                __typename
              }
              __typename
            }
            __typename
          }
          # flowId
          status
          initiator {
            ...Account
            __typename
          }
          outcome {
            ...FlowOutcomeData
            __typename
          }
          timing {
            __typename
          }
          startCondition {
            __typename
            ... on FlowStartConditionThrottling {
              intervalSec
              wakeUpAt
              shiftedFrom
              __typename
            }
            ... on FlowStartConditionBatching {
              activeBatchingRule {
                minRecordsToAwait
                maxBatchingInterval {
                  ...TimeDeltaData
                  __typename
                }
                __typename
              }
              batchingDeadline
              accumulatedRecordsCount
              watermarkModified
              __typename
            }
            ... on FlowStartConditionSchedule {
              wakeUpAt
              __typename
            }
            ... on FlowStartConditionExecutor {
              taskId
              __typename
            }
          }
          configSnapshot {
            ... on FlowConfigurationIngest {
              fetchUncacheable
              __typename
            }
            ... on FlowConfigurationCompactionRule {
              compactionRule {
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }

        fragment Account on Account {
          id
          accountName
          displayName
          accountType
          avatarUrl
          isAdmin
          __typename
        }

        fragment FlowOutcomeData on FlowOutcome {
          ... on FlowSuccessResult {
            message
            __typename
          }
          ... on FlowFailedError {
            reason {
              ... on FlowFailureReasonGeneral {
                message
                __typename
              }
              ... on FlowFailureReasonInputDatasetCompacted {
                message
                inputDataset {
                  ...DatasetBasics
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          ... on FlowAbortedResult {
            message
            __typename
          }
          __typename
        }

        fragment TimeDeltaData on TimeDelta {
          every
          unit
          __typename
        }

        fragment DatasetPageInfo on PageBasedInfo {
          hasNextPage
          hasPreviousPage
          currentPage
          totalPages
          __typename
        }

        fragment FlowConnectionWidgetData on FlowConnection {
          nodes {
            ...FlowItemWidgetData
            __typename
          }
          totalCount
          __typename
        }

        fragment FlowItemWidgetData on Flow {
          status
          initiator {
            accountName
            __typename
          }
          outcome {
            ...FlowOutcomeData
            __typename
          }
          timing {
            # awaitingExecutorSince
            # runningSince
            # finishedAt
            __typename
          }
          __typename
        }
        "#
    )
    .replace("$datasetId", &format!("\"{dataset_id}\""))
    .replace("$page", "0")
    .replace("$perPageTable", "15")
    .replace("$perPageTiles", "150")
    .replace("$filters", "{}")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
