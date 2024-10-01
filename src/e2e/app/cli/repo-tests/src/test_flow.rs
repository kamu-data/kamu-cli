// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{AccessToken, KamuApiServerClient, KamuApiServerClientExt};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_get_dataset_list_flows(kamu_api_server_client: KamuApiServerClient) {
    let token = kamu_api_server_client.login_as_kamu().await;

    let dataset_id = kamu_api_server_client
        .create_player_scores_dataset_with_data(&token)
        .await;

    // The query is almost identical to kamu-web-ui, for ease of later edits.
    // Except for commented dynamic dataset ID fields:
    // - search FlowDescriptionDatasetHardCompaction (datasetId)
    // - search DatasetBasics (id)

    kamu_api_server_client
        .graphql_api_call_assert_with_token(
            token,
            get_dataset_list_flows_query(&dataset_id).as_str(),
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

pub async fn test_dataset_all_flows_paused(kamu_api_server_client: KamuApiServerClient) {
    let token = kamu_api_server_client.login_as_kamu().await;

    let dataset_id = kamu_api_server_client
        .create_player_scores_dataset_with_data(&token)
        .await;

    // The query is almost identical to kamu-web-ui, for ease of later edits.

    kamu_api_server_client
        .graphql_api_call_assert_with_token(
            token,
            indoc::indoc!(
                r#"
                query datasetAllFlowsPaused() {
                  datasets {
                    byId(datasetId: $datasetId) {
                      flows {
                        configs {
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
            .replace("$datasetId", format!("\"{dataset_id}\"").as_str())
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
                        "configs": {
                          "__typename": "DatasetFlowConfigs",
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

pub async fn test_dataset_flows_initiators(kamu_api_server_client: KamuApiServerClient) {
    let token = kamu_api_server_client.login_as_kamu().await;

    let dataset_id = kamu_api_server_client
        .create_player_scores_dataset_with_data(&token)
        .await;

    // The query is almost identical to kamu-web-ui, for ease of later edits.

    kamu_api_server_client
        .graphql_api_call_assert_with_token(
            token,
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
            .replace("$datasetId", format!("\"{dataset_id}\"").as_str())
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

pub async fn test_dataset_trigger_flow(kamu_api_server_client: KamuApiServerClient) {
    let token = kamu_api_server_client.login_as_kamu().await;

    let _root_dataset_id = kamu_api_server_client
        .create_player_scores_dataset_with_data(&token)
        .await;

    let derivative_dataset_id = kamu_api_server_client.create_leaderboard(&token).await;

    // The query is almost identical to kamu-web-ui, for ease of later edits.
    // Except for commented dynamic dataset ID fields:
    // - search FlowDescriptionDatasetPollingIngest (datasetId)
    // - search FlowDescriptionDatasetPushIngest (datasetId)
    // - search FlowDescriptionDatasetExecuteTransform (datasetId)
    // - search FlowDescriptionDatasetHardCompaction (datasetId)
    // - search FlowSummaryData (datasetId, flowId)

    kamu_api_server_client
        .graphql_api_call_assert_with_token(
            token.clone(),
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
                      activeTransformRule {
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
                      schedule {
                        ... on TimeDelta {
                          ...TimeDeltaData
                          __typename
                        }
                        ... on Cron5ComponentExpression {
                          cron5ComponentExpression
                          __typename
                        }
                        __typename
                      }
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
                format!("\"{derivative_dataset_id}\"").as_str(),
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

    wait_for_flows_to_finish(
        &kamu_api_server_client,
        derivative_dataset_id.as_str(),
        token.clone(),
    )
    .await;

    kamu_api_server_client
        .graphql_api_call_assert_with_token(
            token,
            get_dataset_list_flows_query(&derivative_dataset_id).as_str(),
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
                            "engine": "risingwave"
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
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_dataset_list_flows_query(dataset_id: &str) -> String {
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
              activeTransformRule {
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
              schedule {
                ... on TimeDelta {
                  ...TimeDeltaData
                  __typename
                }
                ... on Cron5ComponentExpression {
                  cron5ComponentExpression
                  __typename
                }
                __typename
              }
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
    .replace("$datasetId", format!("\"{dataset_id}\"").as_str())
    .replace("$page", "0")
    .replace("$perPageTable", "15")
    .replace("$perPageTiles", "150")
    .replace("$filters", "{}")
}

async fn wait_for_flows_to_finish(
    kamu_api_server_client: &KamuApiServerClient,
    dataset_id: &str,
    token: AccessToken,
) {
    let retry_strategy = FixedInterval::from_millis(5_000).take(10);

    Retry::spawn(retry_strategy, || async {
        let response = kamu_api_server_client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query getDatasetListFlows() {
                      datasets {
                        byId(datasetId: "<DATASET_ID>") {
                          flows {
                            runs {
                              table: listFlows(
                                page: 0
                                perPage: 10
                              ) {
                                edges {
                                  node {
                                    status
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
                .replace("<DATASET_ID>", dataset_id)
                .as_str(),
                Some(token.clone()),
            )
            .await;

        let edges = response["datasets"]["byId"]["flows"]["runs"]["table"]["edges"]
            .as_array()
            .unwrap();
        let all_finished = edges.iter().all(|edge| {
            let status = edge["node"]["status"].as_str().unwrap();

            status == "FINISHED"
        });

        if all_finished {
            Ok(())
        } else {
            Err(())
        }
    })
    .await
    .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
