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
use kamu::MetadataQueryServiceImpl;
use kamu_core::TenancyConfig;
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, CreateDatasetResult};
use kamu_datasets_services::testing::FakeDependencyGraphIndexer;
use kamu_flow_system::{FlowAgentConfig, FlowSystemEventAgent, FlowSystemEventAgentConfig};
use kamu_flow_system_inmem::{
    InMemoryFlowConfigurationEventStore,
    InMemoryFlowEventStore,
    InMemoryFlowProcessState,
    InMemoryFlowSystemEventBridge,
    InMemoryFlowTriggerEventStore,
};
use odf::metadata::testing::MetadataFactory;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_basic_process_state_actions_root_dataset() {
    let harness = FlowProcessesHarness::new().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(process_summary.flow_type, "INGEST");
    assert_eq!(process_summary.effective_state, "UNCONFIGURED");
    assert_eq!(process_summary.consecutive_failures, 0);

    harness
        .set_time_delta_trigger(&schema, &foo_result.dataset_handle.id, "INGEST")
        .await;

    harness.project_flow_system_events().await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(process_summary.flow_type, "INGEST");
    assert_eq!(process_summary.effective_state, "ACTIVE");
    assert_eq!(process_summary.consecutive_failures, 0);

    harness
        .pause_trigger(&schema, &foo_result.dataset_handle.id, "INGEST")
        .await;

    harness.project_flow_system_events().await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(process_summary.flow_type, "INGEST");
    assert_eq!(process_summary.effective_state, "PAUSED_MANUAL");
    assert_eq!(process_summary.consecutive_failures, 0);

    harness
        .resume_trigger(&schema, &foo_result.dataset_handle.id, "INGEST")
        .await;

    harness.project_flow_system_events().await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(process_summary.flow_type, "INGEST");
    assert_eq!(process_summary.effective_state, "ACTIVE");
    assert_eq!(process_summary.consecutive_failures, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_basic_process_state_actions_derived_dataset() {
    let harness = FlowProcessesHarness::new().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let bar_result = harness
        .create_derived_dataset(bar_alias, vec![foo_result.dataset_handle.id.clone()])
        .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(process_summary.flow_type, "EXECUTE_TRANSFORM");
    assert_eq!(process_summary.effective_state, "UNCONFIGURED");
    assert_eq!(process_summary.consecutive_failures, 0);

    harness
        .set_reactive_trigger(&schema, &bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .await;

    harness.project_flow_system_events().await;

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(process_summary.flow_type, "EXECUTE_TRANSFORM");
    assert_eq!(process_summary.effective_state, "ACTIVE");
    assert_eq!(process_summary.consecutive_failures, 0);

    harness
        .pause_trigger(&schema, &bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .await;

    harness.project_flow_system_events().await;

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(process_summary.flow_type, "EXECUTE_TRANSFORM");
    assert_eq!(process_summary.effective_state, "PAUSED_MANUAL");
    assert_eq!(process_summary.consecutive_failures, 0);

    harness
        .resume_trigger(&schema, &bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .await;

    harness.project_flow_system_events().await;

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(process_summary.flow_type, "EXECUTE_TRANSFORM");
    assert_eq!(process_summary.effective_state, "ACTIVE");
    assert_eq!(process_summary.consecutive_failures, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct FlowProcessesHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    flow_system_event_agent: Arc<dyn FlowSystemEventAgent>,
    _catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl FlowProcessesHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add::<MetadataQueryServiceImpl>()
                .add_value(FlowSystemEventAgentConfig::local_default())
                .add_value(FlowAgentConfig::test_default())
                .add::<InMemoryFlowProcessState>()
                .add::<InMemoryFlowTriggerEventStore>()
                .add::<InMemoryFlowConfigurationEventStore>()
                .add::<InMemoryFlowEventStore>()
                .add::<InMemoryFlowSystemEventBridge>()
                .add::<FakeDependencyGraphIndexer>();

            kamu_flow_system_services::register_dependencies(&mut b);
            // kamu_adapter_flow_dataset::register_dependencies(&mut b, Default::default());

            /*register_message_dispatcher::<ts::TaskProgressMessage>(
                &mut b,
                ts::MESSAGE_PRODUCER_KAMU_TASK_AGENT,
            );

            register_message_dispatcher::<FlowConfigurationUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
            );

            register_message_dispatcher::<FlowTriggerUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
            );*/

            b.build()
        };

        // Init dataset with no sources
        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&catalog_base, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
            flow_system_event_agent: catalog_anonymous.get_one().unwrap(),
            _catalog_anonymous: catalog_anonymous,
            catalog_authorized,
        }
    }

    async fn create_root_dataset(&self, dataset_alias: odf::DatasetAlias) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(dataset_alias)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    async fn create_derived_dataset(
        &self,
        dataset_alias: odf::DatasetAlias,
        inputs: Vec<odf::DatasetID>,
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_alias)
                    .kind(odf::DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(inputs)
                            .build(),
                    )
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    async fn read_flow_process_state(
        &self,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &odf::DatasetID,
    ) -> async_graphql::Value {
        let request_code = r#"
            query($id: DatasetID!) {
                datasets {
                    byId (datasetId: $id) {
                        flows {
                            processes {
                                primary {
                                    flowType
                                    summary {
                                        effectiveState
                                        consecutiveFailures
                                        stopPolicy {
                                            __typename
                                        }
                                        lastSuccessAt
                                        lastAttemptAt
                                        lastFailureAt
                                        nextPlannedAt
                                        autoStoppedReason
                                        autoStoppedAt
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "#;

        let response = schema
            .execute(
                async_graphql::Request::new(request_code)
                    .variables(async_graphql::Variables::from_value(value!({
                        "id": dataset_id.to_string(),
                    })))
                    .data(self.catalog_authorized.clone()),
            )
            .await;

        assert!(response.is_ok(), "{:?}", response.errors);

        response.data
    }

    async fn set_time_delta_trigger(
        &self,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
    ) {
        let mutation_code = r#"
            mutation($id: DatasetID!, $flowType: String!) {
                datasets {
                    byId (datasetId: $id) {
                        flows {
                            triggers {
                                setTrigger (
                                    datasetFlowType: $flowType,
                                    triggerRuleInput: {
                                        schedule: {
                                            timeDelta: { every: 5, unit: "MINUTES" }
                                        }
                                    }
                                    triggerStopPolicyInput: {
                                        never: { dummy: true }
                                    }
                                ) {
                                    __typename
                                }
                            }
                        }
                    }
                }
            }
            "#;

        let response = schema
            .execute(
                async_graphql::Request::new(mutation_code)
                    .variables(async_graphql::Variables::from_value(value!({
                        "id": dataset_id.to_string(),
                        "flowType": flow_type,
                    })))
                    .data(self.catalog_authorized.clone()),
            )
            .await;

        assert!(response.is_ok(), "{:?}", response.errors);
    }

    async fn set_reactive_trigger(
        &self,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
    ) {
        let mutation_code = r#"
            mutation($id: DatasetID!, $flowType: String!) {
                datasets {
                    byId (datasetId: $id) {
                        flows {
                            triggers {
                                setTrigger (
                                    datasetFlowType: $flowType,
                                    triggerRuleInput: {
                                        reactive: {
                                            forNewData: {
                                                buffering: {
                                                    minRecordsToAwait: 1,
                                                    maxBatchingInterval: { every: 10, unit: "MINUTES" }
                                                }
                                            },
                                            forBreakingChange: "NO_ACTION"
                                        }
                                    },
                                    triggerStopPolicyInput: {
                                        never: { dummy: true }
                                    }
                                ) {
                                    __typename
                                }
                            }
                        }
                    }
                }
            }
            "#;

        let response = schema
            .execute(
                async_graphql::Request::new(mutation_code)
                    .variables(async_graphql::Variables::from_value(value!({
                        "id": dataset_id.to_string(),
                        "flowType": flow_type,
                    })))
                    .data(self.catalog_authorized.clone()),
            )
            .await;

        assert!(response.is_ok(), "{:?}", response.errors);
    }

    async fn pause_trigger(
        &self,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
    ) {
        let mutation_code = r#"
            mutation($id: DatasetID!, $flowType: String!) {
                datasets {
                    byId (datasetId: $id) {
                        flows {
                            triggers {
                                pauseFlow (
                                    datasetFlowType: $flowType
                                )
                            }
                        }
                    }
                }
            }
            "#;

        let response = schema
            .execute(
                async_graphql::Request::new(mutation_code)
                    .variables(async_graphql::Variables::from_value(value!({
                        "id": dataset_id.to_string(),
                        "flowType": flow_type,
                    })))
                    .data(self.catalog_authorized.clone()),
            )
            .await;

        assert!(response.is_ok(), "{:?}", response.errors);
    }

    async fn resume_trigger(
        &self,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &odf::DatasetID,
        flow_type: &str,
    ) {
        let mutation_code = r#"
            mutation($id: DatasetID!, $flowType: String!) {
                datasets {
                    byId (datasetId: $id) {
                        flows {
                            triggers {
                                resumeFlow (
                                    datasetFlowType: $flowType
                                )
                            }
                        }
                    }
                }
            }
            "#;

        let response = schema
            .execute(
                async_graphql::Request::new(mutation_code)
                    .variables(async_graphql::Variables::from_value(value!({
                        "id": dataset_id.to_string(),
                        "flowType": flow_type,
                    })))
                    .data(self.catalog_authorized.clone()),
            )
            .await;

        assert!(response.is_ok(), "{:?}", response.errors);
    }

    async fn project_flow_system_events(&self) {
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await
            .unwrap();
    }

    fn extract_primary_flow_process(response: &async_graphql::Value) -> FlowProcessSummaryBasic {
        fn get_obj<'a>(
            value: &'a async_graphql::Value,
            key: &str,
        ) -> Option<&'a async_graphql::Value> {
            if let async_graphql::Value::Object(obj) = value {
                obj.get(key)
            } else {
                None
            }
        }

        // Navigate through the nested structure more compactly
        get_obj(response, "datasets")
            .and_then(|v| get_obj(v, "byId"))
            .and_then(|v| get_obj(v, "flows"))
            .and_then(|v| get_obj(v, "processes"))
            .and_then(|v| get_obj(v, "primary"))
            .map(|primary| FlowProcessSummaryBasic::from(primary.clone()))
            .unwrap_or_else(|| panic!("Invalid GraphQL response structure"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowProcessSummaryBasic {
    flow_type: String,
    effective_state: String,
    consecutive_failures: i32,
}

impl From<async_graphql::Value> for FlowProcessSummaryBasic {
    fn from(value: async_graphql::Value) -> Self {
        // Convert the GraphQL Value to a JSON Value for easier access
        let json_value = value.into_json().unwrap();

        let flow_type = json_value["flowType"].as_str().unwrap().to_string();

        let summary = &json_value["summary"];
        let effective_state = summary["effectiveState"].as_str().unwrap().to_string();

        let consecutive_failures =
            i32::try_from(summary["consecutiveFailures"].as_i64().unwrap()).unwrap();

        Self {
            flow_type,
            effective_state,
            consecutive_failures,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
