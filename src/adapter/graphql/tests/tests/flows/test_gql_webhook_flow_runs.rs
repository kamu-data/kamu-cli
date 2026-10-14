// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use indoc::indoc;
use kamu_adapter_task_webhook::TaskErrorWebhookDelivery;
use kamu_task_system::TaskError;
use kamu_webhooks::{
    DEFAULT_MAX_WEBHOOK_CONSECUTIVE_FAILURES,
    WebhookSendConnectionTimeoutError,
    WebhookSendFailedToConnectError,
    WebhookSubscriptionID,
    WebhookUnsuccessfulResponseError,
};
use pretty_assertions::assert_eq;

use crate::utils::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_flow_run_described_by_its_subscription() {
    let harness = WebhookFlowRunsHarness::new().await;
    let alpha_id = harness.create_subscription("alpha").await;

    harness
        .mimic_webhook_flow_success(&harness.foo_id, alpha_id)
        .await;

    assert_eq!(
        harness.list_webhook_flows().await,
        value!([
            {
                "flowId": "0",
                "status": "FINISHED",
                "description": {
                    "__typename": "FlowDescriptionWebhookDeliver",
                    "targetUrl": "https://example.com/alpha",
                    "label": "alpha",
                    "eventType": "DATASET.REF.UPDATED",
                },
                "outcome": {
                    "__typename": "FlowSuccessResult",
                },
            },
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_flow_run_failure_reasons() {
    let harness = WebhookFlowRunsHarness::new().await;

    let timeout_id = harness.create_subscription("timeout").await;
    let refused_id = harness.create_subscription("refused").await;
    let rejected_id = harness.create_subscription("rejected").await;

    harness
        .mimic_webhook_flow_failure(
            &harness.foo_id,
            timeout_id,
            TaskErrorWebhookDelivery::ConnectionTimeout(WebhookSendConnectionTimeoutError {
                target_url: WebhookFlowRunsHarness::target_url("timeout"),
                timeout: std::time::Duration::from_secs(10),
            })
            .into_task_error(),
        )
        .await;
    harness
        .mimic_webhook_flow_failure(
            &harness.foo_id,
            refused_id,
            WebhookFlowRunsHarness::failed_to_connect("refused"),
        )
        .await;
    harness
        .mimic_webhook_flow_failure(
            &harness.foo_id,
            rejected_id,
            TaskErrorWebhookDelivery::UnsuccessfulResponse(WebhookUnsuccessfulResponseError {
                target_url: WebhookFlowRunsHarness::target_url("rejected"),
                status_code: 500,
            })
            .into_task_error(),
        )
        .await;

    assert_eq!(
        harness.list_webhook_flows().await,
        value!([
            {
                "flowId": "2",
                "status": "FINISHED",
                "description": {
                    "__typename": "FlowDescriptionWebhookDeliver",
                    "targetUrl": "https://example.com/rejected",
                    "label": "rejected",
                    "eventType": "DATASET.REF.UPDATED",
                },
                "outcome": {
                    "__typename": "FlowFailedError",
                    "reason": {
                        "__typename": "TaskFailureReasonWebhookDeliveryProblem",
                        "targetUrl": "https://example.com/rejected",
                        "message": "Unsuccessful response code: 500 Internal Server Error",
                    },
                },
            },
            {
                "flowId": "1",
                "status": "FINISHED",
                "description": {
                    "__typename": "FlowDescriptionWebhookDeliver",
                    "targetUrl": "https://example.com/refused",
                    "label": "refused",
                    "eventType": "DATASET.REF.UPDATED",
                },
                "outcome": {
                    "__typename": "FlowFailedError",
                    "reason": {
                        "__typename": "TaskFailureReasonWebhookDeliveryProblem",
                        "targetUrl": "https://example.com/refused",
                        "message": "Failed to connect",
                    },
                },
            },
            {
                "flowId": "0",
                "status": "FINISHED",
                "description": {
                    "__typename": "FlowDescriptionWebhookDeliver",
                    "targetUrl": "https://example.com/timeout",
                    "label": "timeout",
                    "eventType": "DATASET.REF.UPDATED",
                },
                "outcome": {
                    "__typename": "FlowFailedError",
                    "reason": {
                        "__typename": "TaskFailureReasonWebhookDeliveryProblem",
                        "targetUrl": "https://example.com/timeout",
                        "message": "Response timeout after 10 seconds",
                    },
                },
            },
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_flow_failures_mark_subscription_unreachable_until_reactivated() {
    let harness = WebhookFlowRunsHarness::new().await;
    let alpha_id = harness.create_subscription("alpha").await;

    for _ in 0..DEFAULT_MAX_WEBHOOK_CONSECUTIVE_FAILURES {
        harness
            .mimic_webhook_flow_failure(
                &harness.foo_id,
                alpha_id,
                WebhookFlowRunsHarness::failed_to_connect("alpha"),
            )
            .await;
    }

    assert_eq!(
        harness.webhook_process_summary().await,
        value!([
            {
                "name": "alpha",
                "summary": {
                    "effectiveState": "STOPPED_AUTO",
                    "consecutiveFailures": 5,
                    "autoStoppedReason": "STOP_POLICY",
                },
            },
        ])
    );
    assert_eq!(harness.subscription_status(alpha_id).await, "UNREACHABLE");

    assert_eq!(
        harness.reactivate_subscription(alpha_id).await,
        value!({
            "__typename": "ReactivateWebhookSubscriptionResultSuccess",
            "message": "Success",
        })
    );

    assert_eq!(
        harness.webhook_process_summary().await,
        value!([
            {
                "name": "alpha",
                "summary": {
                    "effectiveState": "ACTIVE",
                    "consecutiveFailures": 0,
                    "autoStoppedReason": null,
                },
            },
        ])
    );
    assert_eq!(harness.subscription_status(alpha_id).await, "ENABLED");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_flow_run_history_shows_upstream_dataset_update() {
    let harness = WebhookFlowRunsHarness::new().await;
    let alpha_id = harness.create_subscription("alpha").await;

    harness
        .mimic_webhook_flow_success(&harness.foo_id, alpha_id)
        .await;

    assert_eq!(
        harness.webhook_flow_history("0").await,
        value!([
            {
                "__typename": "FlowEventInitiated",
                "activationCause": {
                    "__typename": "FlowActivationCauseDatasetUpdate",
                    "datasetId": harness.foo_id.to_string(),
                    "dataset": {
                        "name": "foo",
                    },
                    "source": {
                        "__typename": "FlowActivationCauseDatasetUpdateSourceUpstreamFlow",
                        "flowId": "1",
                    },
                },
            },
            {
                "__typename": "FlowEventStartConditionUpdated",
                "startCondition": {
                    "__typename": "FlowStartConditionReactive",
                },
            },
            {
                "__typename": "FlowEventScheduledForActivation",
            },
            {
                "__typename": "FlowEventStartConditionUpdated",
                "startCondition": {
                    "__typename": "FlowStartConditionExecutor",
                },
            },
            {
                "__typename": "FlowEventTaskChanged",
                "taskStatus": "QUEUED",
            },
            {
                "__typename": "FlowEventTaskChanged",
                "taskStatus": "RUNNING",
            },
            {
                "__typename": "FlowEventTaskChanged",
                "taskStatus": "FINISHED",
            },
            {
                "__typename": "FlowEventCompleted",
            },
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A "foo" root dataset, ready for webhook subscriptions to it
#[oop::extend(BaseGQLFlowRunsHarness, base_gql_flow_runs_harness)]
struct WebhookFlowRunsHarness {
    base_gql_flow_runs_harness: BaseGQLFlowRunsHarness,
    schema: kamu_adapter_graphql::Schema,
    foo_id: odf::DatasetID,
}

impl WebhookFlowRunsHarness {
    async fn new() -> Self {
        let base_gql_flow_runs_harness =
            BaseGQLFlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
                dataset_changes_mock: None,
            })
            .await;

        let foo_id = base_gql_flow_runs_harness
            .create_root_dataset(odf::DatasetAlias::new(
                None,
                odf::DatasetName::new_unchecked("foo"),
            ))
            .await
            .dataset_handle
            .id;

        Self {
            base_gql_flow_runs_harness,
            schema: kamu_adapter_graphql::schema_quiet(),
            foo_id,
        }
    }

    /// Subscribes `https://example.com/<label>` to updates of "foo"
    async fn create_subscription(&self, label: &str) -> WebhookSubscriptionID {
        self.create_webhook_for_dataset_updates(&self.foo_id, label)
            .await
    }

    fn target_url(label: &str) -> url::Url {
        url::Url::parse(&format!("https://example.com/{label}")).unwrap()
    }

    fn failed_to_connect(label: &str) -> TaskError {
        TaskErrorWebhookDelivery::FailedToConnect(WebhookSendFailedToConnectError {
            target_url: Self::target_url(label),
        })
        .into_task_error()
    }

    /// Webhook flow runs of "foo", newest first
    async fn list_webhook_flows(&self) -> async_graphql::Value {
        self.execute(
            indoc!(
                r#"
                query($datasetId: DatasetID!) {
                    datasets {
                        byId (datasetId: $datasetId) {
                            flows {
                                runs {
                                    listFlows(
                                        filters: {
                                            byProcessType: {
                                                webhooks: { subscriptionIds: [] }
                                            }
                                        }
                                    ) {
                                        nodes {
                                            flowId
                                            status
                                            description {
                                                __typename
                                                ... on FlowDescriptionWebhookDeliver {
                                                    targetUrl
                                                    label
                                                    eventType
                                                }
                                            }
                                            outcome {
                                                __typename
                                                ... on FlowFailedError {
                                                    reason {
                                                        __typename
                                                        ... on TaskFailureReasonWebhookDeliveryProblem {
                                                            targetUrl
                                                            message
                                                        }
                                                    }
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
            ),
            value!({ "datasetId": self.foo_id.to_string() }),
            "/datasets/byId/flows/runs/listFlows/nodes",
        )
        .await
    }

    /// History of a webhook flow of "foo", without event IDs and times
    async fn webhook_flow_history(&self, flow_id: &str) -> async_graphql::Value {
        self.execute(
            indoc!(
                r#"
                query($datasetId: DatasetID!, $flowId: String!) {
                    datasets {
                        byId (datasetId: $datasetId) {
                            flows {
                                runs {
                                    getFlow(flowId: $flowId) {
                                        ... on GetFlowSuccess {
                                            flow {
                                                history {
                                                    __typename
                                                    ... on FlowEventInitiated {
                                                        activationCause {
                                                            __typename
                                                            ... on FlowActivationCauseDatasetUpdate {
                                                                datasetId
                                                                dataset {
                                                                    name
                                                                }
                                                                source {
                                                                    __typename
                                                                    ... on FlowActivationCauseDatasetUpdateSourceUpstreamFlow {
                                                                        flowId
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    ... on FlowEventStartConditionUpdated {
                                                        startCondition {
                                                            __typename
                                                        }
                                                    }
                                                    ... on FlowEventTaskChanged {
                                                        taskStatus
                                                    }
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
            ),
            value!({
                "datasetId": self.foo_id.to_string(),
                "flowId": flow_id,
            }),
            "/datasets/byId/flows/runs/getFlow/flow/history",
        )
        .await
    }

    /// Process states of the webhook subscriptions to "foo"
    async fn webhook_process_summary(&self) -> async_graphql::Value {
        self.catchup_flow_system_events().await;

        self.execute(
            indoc!(
                r#"
                query($datasetId: DatasetID!) {
                    datasets {
                        byId (datasetId: $datasetId) {
                            flows {
                                processes {
                                    webhooks {
                                        subprocesses {
                                            name
                                            summary {
                                                effectiveState
                                                consecutiveFailures
                                                autoStoppedReason
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ),
            value!({ "datasetId": self.foo_id.to_string() }),
            "/datasets/byId/flows/processes/webhooks/subprocesses",
        )
        .await
    }

    async fn subscription_status(&self, subscription_id: WebhookSubscriptionID) -> String {
        self.execute(
            indoc!(
                r#"
                query($datasetId: DatasetID!, $subscriptionId: WebhookSubscriptionID!) {
                    datasets {
                        byId (datasetId: $datasetId) {
                            webhooks {
                                subscription(id: $subscriptionId) {
                                    status
                                }
                            }
                        }
                    }
                }
                "#
            ),
            value!({
                "datasetId": self.foo_id.to_string(),
                "subscriptionId": subscription_id.to_string(),
            }),
            "/datasets/byId/webhooks/subscription/status",
        )
        .await
        .into_json()
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
    }

    async fn reactivate_subscription(
        &self,
        subscription_id: WebhookSubscriptionID,
    ) -> async_graphql::Value {
        self.execute(
            indoc!(
                r#"
                mutation($datasetId: DatasetID!, $subscriptionId: WebhookSubscriptionID!) {
                    datasets {
                        byId (datasetId: $datasetId) {
                            webhooks {
                                subscription(id: $subscriptionId) {
                                    reactivate {
                                        __typename
                                        message
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ),
            value!({
                "datasetId": self.foo_id.to_string(),
                "subscriptionId": subscription_id.to_string(),
            }),
            "/datasets/byId/webhooks/subscription/reactivate",
        )
        .await
    }

    /// Runs a request as the logged-in owner and returns the part of its data
    /// at the given JSON pointer
    async fn execute(
        &self,
        request_code: &str,
        variables: async_graphql::Value,
        data_pointer: &str,
    ) -> async_graphql::Value {
        let data = GraphQLQueryRequest::new(
            request_code,
            async_graphql::Variables::from_value(variables),
        )
        .execute(&self.schema, &self.catalog_authorized)
        .await
        .data
        .into_json()
        .unwrap();

        async_graphql::Value::from_json(data.pointer(data_pointer).unwrap().clone()).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
