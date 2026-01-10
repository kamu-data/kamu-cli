// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::value;
use indoc::indoc;
use kamu_core::TenancyConfig;
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use kamu_task_system::{TaskError, TaskOutcome};
use messaging_outbox::OutboxProvider;
use odf::dataset::MetadataChainIncrementInterval;
use pretty_assertions::assert_eq;

use crate::utils::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_primary_rollup() {
    let harness = AccountFlowProcessesHarness::new().await;
    let schema = kamu_adapter_graphql::schema_quiet();

    harness.create_shared_test_case(&schema).await;

    let primary_rollup_response = harness
        .rollup_query("primaryRollup")
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await;
    let primary_rollup = harness.extract_rollup(&primary_rollup_response.data, "primaryRollup");

    assert_eq!(
        primary_rollup,
        &value!({
            "total": 4,
            "active": 1,
            "failing": 1,
            "paused": 1,
            "unconfigured": 0,
            "stopped": 1,
            "worstConsecutiveFailures": 1,
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_rollup() {
    let harness = AccountFlowProcessesHarness::new().await;
    let schema = kamu_adapter_graphql::schema_quiet();

    harness.create_shared_test_case(&schema).await;

    let webhook_rollup_response = harness
        .rollup_query("webhookRollup")
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let webhook_rollup = harness.extract_rollup(&webhook_rollup_response.data, "webhookRollup");

    assert_eq!(
        webhook_rollup,
        &value!({
            "total": 4,
            "active": 1,
            "failing": 1,
            "paused": 1,
            "unconfigured": 0,
            "stopped": 1,
            "worstConsecutiveFailures": 1,
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_full_rollup() {
    let harness = AccountFlowProcessesHarness::new().await;
    let schema = kamu_adapter_graphql::schema_quiet();

    harness.create_shared_test_case(&schema).await;

    let full_rollup_response = harness
        .rollup_query("fullRollup")
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let full_rollup = harness.extract_rollup(&full_rollup_response.data, "fullRollup");

    assert_eq!(
        full_rollup,
        &value!({
            "total": 8,
            "active": 2,
            "failing": 2,
            "paused": 2,
            "unconfigured": 0,
            "stopped": 2,
            "worstConsecutiveFailures": 1,
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_primary_cards() {
    let harness = AccountFlowProcessesHarness::new().await;
    let schema = kamu_adapter_graphql::schema_quiet();

    harness.create_shared_test_case(&schema).await;

    // Unfiltered, unordered, unpaged

    let primary_cards_response = harness
        .primary_cards_query(
            None, /* filters */
            None, /* ordering */
            None, /* pagination */
        )
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await;
    let cards = harness.extract_cards("primaryCards", &primary_cards_response.data);

    assert_eq!(
        cards,
        &value!({
            "nodes": [
                {
                    "dataset": { "name": "bar" },
                    "flowType": "INGEST",
                    "summary": {
                        "effectiveState": "STOPPED_AUTO",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": "UNRECOVERABLE_FAILURE",
                    }
                },
                {
                    "dataset": { "name": "foo" },
                    "flowType": "INGEST",
                    "summary": {
                        "effectiveState": "FAILING",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "dataset": { "name": "baz.daily" },
                    "flowType": "EXECUTE_TRANSFORM",
                    "summary": {
                        "effectiveState": "ACTIVE",
                        "consecutiveFailures": 0,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "dataset": { "name": "baz" },
                    "flowType": "EXECUTE_TRANSFORM",
                    "summary": {
                        "effectiveState": "PAUSED_MANUAL",
                        "consecutiveFailures": 0,
                        "autoStoppedReason": null,
                    }
                },
            ],
            "pageInfo": {
                "hasPreviousPage": false,
                "hasNextPage": false,
                "currentPage": 0,
                "totalPages": 1,
            }
        })
    );

    // Filter by effective state

    let primary_cards_response = harness
        .primary_cards_query(
            Some(value!({
                "effectiveStateIn": [ "FAILING", "PAUSED_MANUAL"]
            })), /* filters */
            None, /* ordering */
            None, /* pagination */
        )
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await;
    let cards = harness.extract_cards("primaryCards", &primary_cards_response.data);

    assert_eq!(
        cards,
        &value!({
            "nodes": [
                {
                    "dataset": { "name": "foo" },
                    "flowType": "INGEST",
                    "summary": {
                        "effectiveState": "FAILING",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "dataset": { "name": "baz" },
                    "flowType": "EXECUTE_TRANSFORM",
                    "summary": {
                        "effectiveState": "PAUSED_MANUAL",
                        "consecutiveFailures": 0,
                        "autoStoppedReason": null,
                    }
                },

            ],
            "pageInfo": {
                "hasPreviousPage": false,
                "hasNextPage": false,
                "currentPage": 0,
                "totalPages": 1,
            }
        })
    );

    // Order by effective state
    let primary_cards_response = harness
        .primary_cards_query(
            None, /* filters */
            Some(value!({
                "field": "EFFECTIVE_STATE",
                "direction": "DESC"
            })), /* ordering */
            None, /* pagination */
        )
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await;
    let cards = harness.extract_cards("primaryCards", &primary_cards_response.data);

    assert_eq!(
        cards,
        &value!({
            "nodes": [
                {
                    "dataset": { "name": "baz.daily" },
                    "flowType": "EXECUTE_TRANSFORM",
                    "summary": {
                        "effectiveState": "ACTIVE",
                        "consecutiveFailures": 0,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "dataset": { "name": "baz" },
                    "flowType": "EXECUTE_TRANSFORM",
                    "summary": {
                        "effectiveState": "PAUSED_MANUAL",
                        "consecutiveFailures": 0,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "dataset": { "name": "foo" },
                    "flowType": "INGEST",
                    "summary": {
                        "effectiveState": "FAILING",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "dataset": { "name": "bar" },
                    "flowType": "INGEST",
                    "summary": {
                        "effectiveState": "STOPPED_AUTO",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": "UNRECOVERABLE_FAILURE",
                    }
                }
            ],
            "pageInfo": {
                "hasPreviousPage": false,
                "hasNextPage": false,
                "currentPage": 0,
                "totalPages": 1,
            }
        })
    );

    // Page by 2

    let primary_cards_response = harness
        .primary_cards_query(
            None,         /* filters */
            None,         /* ordering */
            Some((1, 2)), /* pagination */
        )
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await;
    let cards = harness.extract_cards("primaryCards", &primary_cards_response.data);

    assert_eq!(
        cards,
        &value!({
            "nodes": [
                {
                    "dataset": { "name": "baz.daily" },
                    "flowType": "EXECUTE_TRANSFORM",
                    "summary": {
                        "effectiveState": "ACTIVE",
                        "consecutiveFailures": 0,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "dataset": { "name": "baz" },
                    "flowType": "EXECUTE_TRANSFORM",
                    "summary": {
                        "effectiveState": "PAUSED_MANUAL",
                        "consecutiveFailures": 0,
                        "autoStoppedReason": null,
                    }
                },
            ],
            "pageInfo": {
                "hasPreviousPage": true,
                "hasNextPage": false,
                "currentPage": 1,
                "totalPages": 2,
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_webhook_cards() {
    let harness = AccountFlowProcessesHarness::new().await;
    let schema = kamu_adapter_graphql::schema_quiet();

    harness.create_shared_test_case(&schema).await;

    // Unfiltered, unordered, unpaged

    let webhook_cards_response = harness
        .webhook_cards_query(
            None, /* filters */
            None, /* ordering */
            None, /* pagination */
        )
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await;
    let cards = harness.extract_cards("webhookCards", &webhook_cards_response.data);

    assert_eq!(
        cards,
        &value!({
            "nodes": [
                {
                    "name": "foo_updates",
                    "parentDataset": { "name": "foo" },
                    "summary": {
                        "effectiveState": "FAILING",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "name": "bar_updates",
                    "parentDataset": { "name": "bar" },
                    "summary": {
                        "effectiveState": "STOPPED_AUTO",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": "UNRECOVERABLE_FAILURE",
                    }
                },
                {
                    "name": "baz_daily_updates",
                    "parentDataset": { "name": "baz.daily" },
                    "summary": {
                        "effectiveState": "ACTIVE",
                        "consecutiveFailures": 0,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "name": "baz_updates",
                    "parentDataset": { "name": "baz" },
                    "summary": {
                        "effectiveState": "PAUSED_MANUAL",
                        "consecutiveFailures": 0,
                        "autoStoppedReason": null,
                    }
                },
            ],
            "pageInfo": {
                "hasPreviousPage": false,
                "hasNextPage": false,
                "currentPage": 0,
                "totalPages": 1
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_all_cards() {
    let harness = AccountFlowProcessesHarness::new().await;
    let schema = kamu_adapter_graphql::schema_quiet();

    harness.create_shared_test_case(&schema).await;

    // Unfiltered, unordered, unpaged

    let all_cards_response = harness
        .all_cards_query(
            None, /* filters */
            Some(value!({
                "field": "EFFECTIVE_STATE",
                "direction": "ASC"
            })), /* ordering */
            Some((0, 4)), /* pagination */
        )
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await;
    let cards = harness.extract_cards("allCards", &all_cards_response.data);

    assert_eq!(
        cards,
        &value!({
            "nodes": [
                {
                    "__typename": "DatasetFlowProcess",
                    "dataset": { "name": "bar" },
                    "flowType": "INGEST",
                    "summary": {
                        "effectiveState": "STOPPED_AUTO",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": "UNRECOVERABLE_FAILURE",
                    }
                },
                {
                    "__typename": "WebhookFlowSubProcess",
                    "name": "bar_updates",
                    "parentDataset": {
                        "name": "bar"
                    },
                    "summary": {
                        "effectiveState": "STOPPED_AUTO",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": "UNRECOVERABLE_FAILURE",
                    }
                },
                {
                    "__typename": "WebhookFlowSubProcess",
                    "name": "foo_updates",
                    "parentDataset": { "name": "foo" },
                    "summary": {
                        "effectiveState": "FAILING",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": null,
                    }
                },
                {
                    "__typename": "DatasetFlowProcess",
                    "dataset": { "name": "foo" },
                    "flowType": "INGEST",
                    "summary": {
                        "effectiveState": "FAILING",
                        "consecutiveFailures": 1,
                        "autoStoppedReason": null,
                    }
                },
            ],
            "pageInfo": {
                "hasPreviousPage": false,
                "hasNextPage": true,
                "currentPage": 0,
                "totalPages": 2
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLFlowRunsHarness, base_gql_flow_runs_harness)]
struct AccountFlowProcessesHarness {
    base_gql_flow_runs_harness: BaseGQLFlowRunsHarness,
    flow_system_event_agent: Arc<dyn FlowSystemEventAgent>,
}

impl AccountFlowProcessesHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .outbox_provider(OutboxProvider::Immediate {
                force_immediate: true,
            })
            .build();

        let base_gql_flow_catalog =
            BaseGQLFlowHarness::make_base_gql_flow_catalog(base_gql_harness.catalog());

        let base_gql_flow_runs_catalog = BaseGQLFlowRunsHarness::make_base_gql_flow_runs_catalog(
            &base_gql_flow_catalog,
            FlowRunsHarnessOverrides {
                dataset_changes_mock: Some(
                    MockDatasetIncrementQueryService::with_increment_between(
                        MetadataChainIncrementInterval {
                            num_blocks: 1,
                            num_records: 10,
                            updated_watermark: None,
                        },
                    ),
                ),
            },
        );

        let base_gql_flow_runs_harness =
            BaseGQLFlowRunsHarness::new(base_gql_harness, base_gql_flow_runs_catalog.clone()).await;

        Self {
            base_gql_flow_runs_harness,
            flow_system_event_agent: base_gql_flow_runs_catalog
                .get_one::<dyn FlowSystemEventAgent>()
                .unwrap(),
        }
    }

    async fn create_shared_test_case(&self, schema: &kamu_adapter_graphql::Schema) {
        // Create hierarchy of 4 datasets

        let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
        let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
        let baz_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));
        let baz_daily_alias =
            odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz.daily"));

        let foo = self.create_root_dataset(foo_alias.clone()).await;
        let bar = self.create_root_dataset(bar_alias.clone()).await;

        let baz = self
            .create_derived_dataset(baz_alias.clone(), &[foo_alias, bar_alias])
            .await;

        let baz_daily = self
            .create_derived_dataset(baz_daily_alias, &[baz_alias])
            .await;

        // Each should have a webhook subscription
        let subscription_id_foo = self
            .create_webhook_for_dataset_updates(&foo.dataset_handle.id, "foo_updates")
            .await;
        let subscription_id_bar = self
            .create_webhook_for_dataset_updates(&bar.dataset_handle.id, "bar_updates")
            .await;
        let subscription_id_baz = self
            .create_webhook_for_dataset_updates(&baz.dataset_handle.id, "baz_updates")
            .await;
        let _subscription_id_baz_daily = self
            .create_webhook_for_dataset_updates(&baz_daily.dataset_handle.id, "baz_daily_updates")
            .await;

        // Flow 0
        self.set_time_delta_trigger(
            &foo.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            Some(value!(
                {
                    "afterConsecutiveFailures": {
                        "maxFailures": 3
                    }
                }
            )),
        )
        .execute(schema, &self.catalog_authorized)
        .await;

        // Flow 1
        self.set_time_delta_trigger(
            &bar.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            Some(value!(
                {
                    "afterConsecutiveFailures": {
                        "maxFailures": 3
                    }
                }
            )),
        )
        .execute(schema, &self.catalog_authorized)
        .await;

        self.set_reactive_trigger_immediate(&baz.dataset_handle.id, "EXECUTE_TRANSFORM", false)
            .execute(schema, &self.catalog_authorized)
            .await;

        self.set_reactive_trigger_buffering(
            &baz_daily.dataset_handle.id,
            "EXECUTE_TRANSFORM",
            1000,
            (1, "HOURS"),
            false,
        )
        .execute(schema, &self.catalog_authorized)
        .await;

        self.mimic_flow_run_with_outcome("0", TaskOutcome::Failed(TaskError::empty_recoverable()))
            .await;
        self.mimic_flow_run_with_outcome(
            "1",
            TaskOutcome::Failed(TaskError::empty_unrecoverable()),
        )
        .await;

        self.pause_flow_trigger(&baz.dataset_handle.id, "EXECUTE_TRANSFORM")
            .execute(schema, &self.catalog_authorized)
            .await;

        self.mimic_webhook_flow_failure(&foo.dataset_handle.id, subscription_id_foo, false)
            .await;
        self.mimic_webhook_flow_failure(&bar.dataset_handle.id, subscription_id_bar, true)
            .await;

        self.pause_webhook_subscription(subscription_id_baz).await;
    }

    async fn rollup_query(&self, rollup_name: &str) -> GraphQLQueryRequest {
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await
            .unwrap();

        let request_code = indoc!(
            r#"
            query {
                accounts {
                    me {
                        flows {
                            processes {
                                <rollup_name> {
                                    total
                                    active
                                    failing
                                    paused
                                    unconfigured
                                    stopped
                                    worstConsecutiveFailures
                                }
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<rollup_name>", rollup_name);

        GraphQLQueryRequest::new(&request_code, async_graphql::Variables::default())
    }

    fn extract_rollup<'a>(
        &self,
        rollup_response: &'a async_graphql::Value,
        rollup_name: &str,
    ) -> &'a async_graphql::Value {
        get_gql_value_property(rollup_response, "accounts")
            .and_then(|v| get_gql_value_property(v, "me"))
            .and_then(|v| get_gql_value_property(v, "flows"))
            .and_then(|v| get_gql_value_property(v, "processes"))
            .and_then(|v| get_gql_value_property(v, rollup_name))
            .unwrap_or_else(|| panic!("Invalid GraphQL response structure"))
    }

    async fn primary_cards_query(
        &self,
        maybe_filters: Option<async_graphql::Value>,
        maybe_ordering: Option<async_graphql::Value>,
        maybe_pagination: Option<(u32, u32)>,
    ) -> GraphQLQueryRequest {
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await
            .unwrap();

        let request_code = indoc!(
            r#"
            query($filters: FlowProcessFilters, $ordering: FlowProcessOrdering, $page: Int, $perPage: Int) {
                accounts {
                    me {
                        flows {
                            processes {
                                primaryCards(filters: $filters, ordering: $ordering, page: $page, perPage: $perPage) {
                                    nodes {
                                        dataset {
                                            name
                                        }
                                        flowType
                                        summary {
                                            effectiveState
                                            consecutiveFailures
                                            autoStoppedReason
                                        }
                                    }
                                    pageInfo {
                                        hasPreviousPage
                                        hasNextPage
                                        currentPage
                                        totalPages
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "#
        );

        let mut vars = async_graphql::Variables::default();
        if let Some(filters) = maybe_filters {
            vars.insert(async_graphql::Name::new("filters"), filters);
        }
        if let Some(ordering) = maybe_ordering {
            vars.insert(async_graphql::Name::new("ordering"), ordering);
        }
        if let Some(pagination) = maybe_pagination {
            vars.insert(async_graphql::Name::new("page"), value!(pagination.0));
            vars.insert(async_graphql::Name::new("perPage"), value!(pagination.1));
        }

        GraphQLQueryRequest::new(request_code, vars)
    }

    async fn webhook_cards_query(
        &self,
        maybe_filters: Option<async_graphql::Value>,
        maybe_ordering: Option<async_graphql::Value>,
        maybe_pagination: Option<(u32, u32)>,
    ) -> GraphQLQueryRequest {
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await
            .unwrap();

        let request_code = indoc!(
            r#"
            query($filters: FlowProcessFilters, $ordering: FlowProcessOrdering, $page: Int, $perPage: Int) {
                accounts {
                    me {
                        flows {
                            processes {
                                webhookCards(filters: $filters, ordering: $ordering, page: $page, perPage: $perPage) {
                                    nodes {
                                        name
                                        parentDataset {
                                            name
                                        }
                                        summary {
                                            effectiveState
                                            consecutiveFailures
                                            autoStoppedReason
                                        }
                                    }
                                    pageInfo {
                                        hasPreviousPage
                                        hasNextPage
                                        currentPage
                                        totalPages
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "#
        );

        let mut vars = async_graphql::Variables::default();
        if let Some(filters) = maybe_filters {
            vars.insert(async_graphql::Name::new("filters"), filters);
        }
        if let Some(ordering) = maybe_ordering {
            vars.insert(async_graphql::Name::new("ordering"), ordering);
        }
        if let Some(pagination) = maybe_pagination {
            vars.insert(async_graphql::Name::new("page"), value!(pagination.0));
            vars.insert(async_graphql::Name::new("perPage"), value!(pagination.1));
        }

        GraphQLQueryRequest::new(request_code, vars)
    }

    async fn all_cards_query(
        &self,
        maybe_filters: Option<async_graphql::Value>,
        maybe_ordering: Option<async_graphql::Value>,
        maybe_pagination: Option<(u32, u32)>,
    ) -> GraphQLQueryRequest {
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await
            .unwrap();

        let request_code = indoc!(
            r#"
            query($filters: FlowProcessFilters, $ordering: FlowProcessOrdering, $page: Int, $perPage: Int) {
                accounts {
                    me {
                        flows {
                            processes {
                                allCards(filters: $filters, ordering: $ordering, page: $page, perPage: $perPage) {
                                    nodes {
                                        __typename
                                        ... on DatasetFlowProcess {
                                            dataset {
                                                name
                                            }
                                            flowType
                                            summary {
                                                effectiveState
                                                consecutiveFailures
                                                autoStoppedReason
                                            }
                                        }
                                        ... on WebhookFlowSubProcess {
                                            name
                                            parentDataset {
                                                name
                                            }
                                            summary {
                                                effectiveState
                                                consecutiveFailures
                                                autoStoppedReason
                                            }
                                        }
                                    }
                                    pageInfo {
                                        hasPreviousPage
                                        hasNextPage
                                        currentPage
                                        totalPages
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "#
        );

        let mut vars = async_graphql::Variables::default();
        if let Some(filters) = maybe_filters {
            vars.insert(async_graphql::Name::new("filters"), filters);
        }
        if let Some(ordering) = maybe_ordering {
            vars.insert(async_graphql::Name::new("ordering"), ordering);
        }
        if let Some(pagination) = maybe_pagination {
            vars.insert(async_graphql::Name::new("page"), value!(pagination.0));
            vars.insert(async_graphql::Name::new("perPage"), value!(pagination.1));
        }

        GraphQLQueryRequest::new(request_code, vars)
    }

    fn extract_cards<'a>(
        &self,
        cards_query: &'static str,
        cards_response: &'a async_graphql::Value,
    ) -> &'a async_graphql::Value {
        get_gql_value_property(cards_response, "accounts")
            .and_then(|v| get_gql_value_property(v, "me"))
            .and_then(|v| get_gql_value_property(v, "flows"))
            .and_then(|v| get_gql_value_property(v, "processes"))
            .and_then(|v| get_gql_value_property(v, cards_query))
            .unwrap_or_else(|| panic!("Invalid GraphQL response structure"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
