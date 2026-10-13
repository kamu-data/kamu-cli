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
use kamu_accounts::CurrentAccountSubject;
use kamu_adapter_flow_dataset::transform_dataset_binding;
use kamu_adapter_flow_webhook::webhook_deliver_binding;
use kamu_auth_rebac::{AccountToDatasetRelation, RebacService};
use kamu_core::TenancyConfig;
use kamu_flow_system::FlowID;
use kamu_task_system::{TaskOutcome, TaskResult};
use kamu_webhooks::{WebhookEventTypeCatalog, WebhookSubscriptionID};
use pretty_assertions::assert_eq;
use serde_json::json;

use crate::utils::{BaseGQLFlowRunsHarness, FlowRunsHarnessOverrides, GraphQLQueryRequest};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completed_flow_lists_activated_transform() {
    let harness = DownstreamLinksHarness::new(TenancyConfig::SingleTenant).await;
    let foo = harness.create_root("foo").await;
    let bar = harness.create_derived("bar", "foo").await;

    let ingest_flow_id = harness.run_completed_ingest_flow(&foo).await;
    let transform_flow_id = harness
        .run_transform_activated_by(&bar, &foo, ingest_flow_id)
        .await;
    harness.catchup_flow_system_events().await;

    harness
        .assert_downstream_flows(
            &harness.catalog_authorized,
            &foo,
            ingest_flow_id,
            json!([DownstreamLinksHarness::accessible_link(
                transform_flow_id,
                &bar,
                "bar",
                "FlowDescriptionDatasetExecuteTransform"
            ),]),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completed_flow_without_downstream_flows() {
    let harness = DownstreamLinksHarness::new(TenancyConfig::SingleTenant).await;
    let foo = harness.create_root("foo").await;

    let ingest_flow_id = harness.run_completed_ingest_flow(&foo).await;
    harness.catchup_flow_system_events().await;

    harness
        .assert_downstream_flows(&harness.catalog_authorized, &foo, ingest_flow_id, json!([]))
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completed_flow_lists_transforms_and_webhook_delivery() {
    let harness = DownstreamLinksHarness::new(TenancyConfig::SingleTenant).await;
    let foo = harness.create_root("foo").await;
    let bar = harness.create_derived("bar", "foo").await;
    let baz = harness.create_derived("baz", "foo").await;
    let subscription_id = harness
        .create_webhook_for_dataset_updates(&foo.id, "alpha")
        .await;

    let ingest_flow_id = harness.run_completed_ingest_flow(&foo).await;
    let bar_flow_id = harness
        .run_transform_activated_by(&bar, &foo, ingest_flow_id)
        .await;
    let baz_flow_id = harness
        .run_transform_activated_by(&baz, &foo, ingest_flow_id)
        .await;
    let webhook_flow_id = harness
        .run_webhook_delivery_activated_by(subscription_id, &foo, ingest_flow_id)
        .await;
    harness.catchup_flow_system_events().await;

    harness
        .assert_downstream_flows(
            &harness.catalog_authorized,
            &foo,
            ingest_flow_id,
            json!([
                DownstreamLinksHarness::accessible_link(
                    bar_flow_id,
                    &bar,
                    "bar",
                    "FlowDescriptionDatasetExecuteTransform"
                ),
                DownstreamLinksHarness::accessible_link(
                    baz_flow_id,
                    &baz,
                    "baz",
                    "FlowDescriptionDatasetExecuteTransform"
                ),
                DownstreamLinksHarness::accessible_link(
                    webhook_flow_id,
                    &foo,
                    "foo",
                    "FlowDescriptionWebhookDeliver"
                ),
            ]),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_batching_two_upstream_flows_is_listed_by_both() {
    let harness = DownstreamLinksHarness::new(TenancyConfig::SingleTenant).await;
    let foo = harness.create_root("foo").await;
    let bar = harness.create_derived("bar", "foo").await;

    let first_ingest_flow_id = harness.run_completed_ingest_flow(&foo).await;
    let transform_flow_id = harness
        .run_transform_activated_by(&bar, &foo, first_ingest_flow_id)
        .await;
    let second_ingest_flow_id = harness.run_completed_ingest_flow(&foo).await;
    let batched_flow_id = harness
        .run_transform_activated_by(&bar, &foo, second_ingest_flow_id)
        .await;
    assert_eq!(batched_flow_id, transform_flow_id);
    harness.catchup_flow_system_events().await;

    for ingest_flow_id in [first_ingest_flow_id, second_ingest_flow_id] {
        harness
            .assert_downstream_flows(
                &harness.catalog_authorized,
                &foo,
                ingest_flow_id,
                json!([DownstreamLinksHarness::accessible_link(
                    transform_flow_id,
                    &bar,
                    "bar",
                    "FlowDescriptionDatasetExecuteTransform"
                ),]),
            )
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_downstream_dataset_keeps_link_without_flow() {
    let harness = DownstreamLinksHarness::new(TenancyConfig::SingleTenant).await;
    let foo = harness.create_root("foo").await;
    let bar = harness.create_derived("bar", "foo").await;

    let ingest_flow_id = harness.run_completed_ingest_flow(&foo).await;
    let transform_flow_id = harness
        .run_transform_activated_by(&bar, &foo, ingest_flow_id)
        .await;
    harness.catchup_flow_system_events().await;

    harness.delete_dataset(&bar).await;
    harness.catchup_flow_system_events().await;

    harness
        .assert_downstream_flows(
            &harness.catalog_authorized,
            &foo,
            ingest_flow_id,
            json!([DownstreamLinksHarness::not_accessible_link(
                transform_flow_id,
                &bar
            )]),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_downstream_flows_follow_caller_access() {
    let harness = DownstreamLinksHarness::new(TenancyConfig::MultiTenant).await;
    let foo = harness.create_root("foo").await;
    let bar = harness.create_derived("bar", "foo").await;
    let subscription_id = harness
        .create_webhook_for_dataset_updates(&foo.id, "alpha")
        .await;

    let ingest_flow_id = harness.run_completed_ingest_flow(&foo).await;
    let transform_flow_id = harness
        .run_transform_activated_by(&bar, &foo, ingest_flow_id)
        .await;
    let webhook_flow_id = harness
        .run_webhook_delivery_activated_by(subscription_id, &foo, ingest_flow_id)
        .await;
    harness.catchup_flow_system_events().await;

    harness
        .assert_downstream_flows(
            &harness.catalog_authorized,
            &foo,
            ingest_flow_id,
            json!([
                DownstreamLinksHarness::accessible_link(
                    transform_flow_id,
                    &bar,
                    "bar",
                    "FlowDescriptionDatasetExecuteTransform"
                ),
                DownstreamLinksHarness::accessible_link(
                    webhook_flow_id,
                    &foo,
                    "foo",
                    "FlowDescriptionWebhookDeliver"
                ),
            ]),
        )
        .await;

    // A reader of the upstream: the private downstream dataset stays hidden, and
    // so does the webhook delivery, which needs Maintain
    let reader_catalog = harness.reader_catalog(&foo).await;
    harness
        .assert_downstream_flows(
            &reader_catalog,
            &foo,
            ingest_flow_id,
            json!([
                DownstreamLinksHarness::not_accessible_link(transform_flow_id, &bar),
                {
                    "flowId": webhook_flow_id.to_string(),
                    "datasetId": foo.id.to_string(),
                    "dataset": DownstreamLinksHarness::accessible_dataset("foo"),
                    "flow": null,
                },
            ]),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_activation_cause_hides_unreadable_upstream_dataset() {
    let harness = DownstreamLinksHarness::new(TenancyConfig::MultiTenant).await;
    let foo = harness.create_root("foo").await;
    let bar = harness.create_derived("bar", "foo").await;

    let ingest_flow_id = harness.run_completed_ingest_flow(&foo).await;
    let transform_flow_id = harness
        .run_transform_activated_by(&bar, &foo, ingest_flow_id)
        .await;

    harness
        .assert_primary_activation_cause(
            &harness.catalog_authorized,
            &bar,
            transform_flow_id,
            json!({
                "datasetId": foo.id.to_string(),
                "dataset": {
                    "name": "foo",
                },
            }),
        )
        .await;

    let reader_catalog = harness.reader_catalog(&bar).await;
    harness
        .assert_primary_activation_cause(
            &reader_catalog,
            &bar,
            transform_flow_id,
            json!({
                "datasetId": foo.id.to_string(),
                "dataset": null,
            }),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLFlowRunsHarness, base_gql_flow_runs_harness)]
struct DownstreamLinksHarness {
    base_gql_flow_runs_harness: BaseGQLFlowRunsHarness,
}

impl DownstreamLinksHarness {
    async fn new(tenancy_config: TenancyConfig) -> Self {
        Self {
            base_gql_flow_runs_harness: BaseGQLFlowRunsHarness::with_tenancy(
                tenancy_config,
                FlowRunsHarnessOverrides::default(),
            )
            .await,
        }
    }

    async fn create_root(&self, name: &str) -> odf::DatasetHandle {
        self.base_gql_flow_runs_harness
            .create_root_dataset(Self::alias(name))
            .await
            .dataset_handle
    }

    async fn create_derived(&self, name: &str, input_name: &str) -> odf::DatasetHandle {
        self.base_gql_flow_runs_harness
            .create_derived_dataset(Self::alias(name), &[Self::alias(input_name)])
            .await
            .dataset_handle
    }

    fn alias(name: &str) -> odf::DatasetAlias {
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(name))
    }

    async fn run_completed_ingest_flow(&self, dataset_handle: &odf::DatasetHandle) -> FlowID {
        self.manually_trigger_flow(
            &dataset_handle.id,
            "INGEST",
            TaskOutcome::Success(TaskResult::empty()),
        )
        .await
    }

    async fn run_transform_activated_by(
        &self,
        dataset_handle: &odf::DatasetHandle,
        upstream_dataset_handle: &odf::DatasetHandle,
        upstream_flow_id: FlowID,
    ) -> FlowID {
        self.run_flow_activated_by_upstream(
            &transform_dataset_binding(&dataset_handle.id),
            &upstream_dataset_handle.id,
            upstream_flow_id,
        )
        .await
    }

    async fn run_webhook_delivery_activated_by(
        &self,
        subscription_id: WebhookSubscriptionID,
        upstream_dataset_handle: &odf::DatasetHandle,
        upstream_flow_id: FlowID,
    ) -> FlowID {
        self.run_flow_activated_by_upstream(
            &webhook_deliver_binding(
                subscription_id,
                &WebhookEventTypeCatalog::dataset_ref_updated(),
                Some(&upstream_dataset_handle.id),
            ),
            &upstream_dataset_handle.id,
            upstream_flow_id,
        )
        .await
    }

    /// Catalog of another account, with the Reader role on the given dataset
    async fn reader_catalog(&self, dataset_handle: &odf::DatasetHandle) -> dill::Catalog {
        let reader_subject = CurrentAccountSubject::new_test_with(&"reader");

        self.catalog_authorized
            .get_one::<dyn RebacService>()
            .unwrap()
            .set_account_dataset_relation(
                reader_subject.account_id(),
                AccountToDatasetRelation::Reader,
                &dataset_handle.id,
            )
            .await
            .unwrap();

        self.catalog_for_subject(reader_subject)
    }

    fn accessible_link(
        flow_id: FlowID,
        dataset_handle: &odf::DatasetHandle,
        dataset_name: &str,
        description_typename: &str,
    ) -> serde_json::Value {
        json!({
            "flowId": flow_id.to_string(),
            "datasetId": dataset_handle.id.to_string(),
            "dataset": Self::accessible_dataset(dataset_name),
            "flow": {
                "flowId": flow_id.to_string(),
                "description": {
                    "__typename": description_typename,
                },
            },
        })
    }

    fn accessible_dataset(dataset_name: &str) -> serde_json::Value {
        json!({
            "__typename": "DatasetAccessResultAccessible",
            "dataset": {
                "name": dataset_name,
            },
        })
    }

    fn not_accessible_link(
        flow_id: FlowID,
        dataset_handle: &odf::DatasetHandle,
    ) -> serde_json::Value {
        json!({
            "flowId": flow_id.to_string(),
            "datasetId": dataset_handle.id.to_string(),
            "dataset": {
                "__typename": "DatasetAccessResultNotAccessible",
                "id": dataset_handle.id.to_string(),
            },
            "flow": null,
        })
    }

    /// Asserts both `Flow.downstreamFlows` and the completion event's
    /// `downstreamFlows` of the upstream flow
    async fn assert_downstream_flows(
        &self,
        catalog: &dill::Catalog,
        upstream_dataset_handle: &odf::DatasetHandle,
        upstream_flow_id: FlowID,
        expected_links: serde_json::Value,
    ) {
        let response = Self::downstream_flows_query(&upstream_dataset_handle.id, upstream_flow_id)
            .execute(&kamu_adapter_graphql::schema_quiet(), catalog)
            .await;
        assert!(response.is_ok(), "{:?}", response.errors);

        let response_json = response.data.into_json().unwrap();
        let flow = &response_json["datasets"]["byId"]["flows"]["runs"]["getFlow"]["flow"];
        let completed_events = flow["history"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["__typename"] == "FlowEventCompleted")
            .collect::<Vec<_>>();

        assert_eq!(completed_events.len(), 1);
        assert_eq!(completed_events[0]["downstreamFlows"], expected_links);
        assert_eq!(flow["downstreamFlows"], expected_links);
    }

    async fn assert_primary_activation_cause(
        &self,
        catalog: &dill::Catalog,
        dataset_handle: &odf::DatasetHandle,
        flow_id: FlowID,
        expected_cause: serde_json::Value,
    ) {
        let response = Self::primary_activation_cause_query(&dataset_handle.id, flow_id)
            .execute(&kamu_adapter_graphql::schema_quiet(), catalog)
            .await;
        assert!(response.is_ok(), "{:?}", response.errors);

        let response_json = response.data.into_json().unwrap();
        assert_eq!(
            response_json["datasets"]["byId"]["flows"]["runs"]["getFlow"]["flow"]
                ["primaryActivationCause"],
            expected_cause
        );
    }

    fn primary_activation_cause_query(
        dataset_id: &odf::DatasetID,
        flow_id: FlowID,
    ) -> GraphQLQueryRequest {
        let query_code = indoc!(
            r#"
            query($datasetId: DatasetID!, $flowId: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                getFlow(flowId: $flowId) {
                                    ... on GetFlowSuccess {
                                        flow {
                                            primaryActivationCause {
                                                ... on FlowActivationCauseDatasetUpdate {
                                                    datasetId
                                                    dataset {
                                                        name
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
        );

        GraphQLQueryRequest::new(
            query_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "flowId": flow_id.to_string(),
            })),
        )
    }

    fn downstream_flows_query(dataset_id: &odf::DatasetID, flow_id: FlowID) -> GraphQLQueryRequest {
        let query_code = indoc!(
            r#"
            fragment DownstreamFlow on FlowDownstreamLink {
                flowId
                datasetId
                dataset {
                    __typename
                    ... on DatasetAccessResultAccessible {
                        dataset {
                            name
                        }
                    }
                    ... on DatasetAccessResultNotAccessible {
                        id
                    }
                }
                flow {
                    flowId
                    description {
                        __typename
                    }
                }
            }

            query($datasetId: DatasetID!, $flowId: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                getFlow(flowId: $flowId) {
                                    ... on GetFlowSuccess {
                                        flow {
                                            downstreamFlows {
                                                ...DownstreamFlow
                                            }
                                            history {
                                                __typename
                                                ... on FlowEventCompleted {
                                                    downstreamFlows {
                                                        ...DownstreamFlow
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
        );

        GraphQLQueryRequest::new(
            query_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "flowId": flow_id.to_string(),
            })),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
