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
use chrono::{DateTime, Duration, DurationRound, Utc};
use kamu::{MetadataQueryServiceImpl, TransformRequestPlannerImpl};
use kamu_adapter_task_dataset::TaskResultDatasetUpdate;
use kamu_core::{PullResult, TenancyConfig};
use kamu_datasets::{
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    DatasetIncrementQueryService,
    DatasetIntervalIncrement,
};
use kamu_datasets_services::testing::{
    FakeDependencyGraphIndexer,
    MockDatasetIncrementQueryService,
};
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_task_system::*;
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::TaskSchedulerImpl;
use messaging_outbox::{Outbox, OutboxExt, register_message_dispatcher};
use odf::metadata::testing::MetadataFactory;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_basic_process_state_actions_root_dataset() {
    let harness = FlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Confirm initial primary process state
    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "UNCONFIGURED".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Configure trigger and confirm state transitions
    harness
        .set_time_delta_trigger(&schema, &foo_result.dataset_handle.id, "INGEST")
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Pause trigger and confirm state transitions

    harness
        .pause_trigger(&schema, &foo_result.dataset_handle.id, "INGEST")
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "PAUSED_MANUAL".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Resume trigger and confirm state transitions

    harness
        .resume_trigger(&schema, &foo_result.dataset_handle.id, "INGEST")
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_basic_process_state_actions_derived_dataset() {
    let harness = FlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    // Create derived dataset
    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let bar_result = harness
        .create_derived_dataset(bar_alias, vec![foo_result.dataset_handle.id.clone()])
        .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Confirm initial primary process state

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            effective_state: "UNCONFIGURED".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Configure trigger and confirm state transitions

    harness
        .set_reactive_trigger(&schema, &bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .await;

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Pause trigger and confirm state transitions

    harness
        .pause_trigger(&schema, &bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .await;

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            effective_state: "PAUSED_MANUAL".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Resume trigger and confirm state transitions

    harness
        .resume_trigger(&schema, &bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .await;

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_several_runs() {
    let harness = FlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Configure trigger
    harness
        .set_time_delta_trigger(&schema, &foo_result.dataset_handle.id, "INGEST")
        .await;

    // Mimic a successful 1st run
    harness
        .mimic_flow_run_with_outcome(
            "0",
            TaskOutcome::Success(
                TaskResultDatasetUpdate {
                    pull_result: PullResult::Updated {
                        old_head: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    // Read latest state

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Mimic a failed 2nd run
    harness
        .mimic_flow_run_with_outcome("1", TaskOutcome::Failed(TaskError::empty_recoverable()))
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "FAILING".to_string(),
            consecutive_failures: 1,
            maybe_auto_stop_reason: None,
        }
    );

    // Mimic a failed 3rd run
    harness
        .mimic_flow_run_with_outcome("2", TaskOutcome::Failed(TaskError::empty_recoverable()))
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "FAILING".to_string(),
            consecutive_failures: 2,
            maybe_auto_stop_reason: None,
        }
    );

    // 4th run succeeds
    harness
        .mimic_flow_run_with_outcome(
            "3",
            TaskOutcome::Success(
                TaskResultDatasetUpdate {
                    pull_result: PullResult::Updated {
                        old_head: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-2"),
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_reach_auto_stop_via_failures_count() {
    let harness = FlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Configure trigger
    harness
        .set_time_delta_trigger(&schema, &foo_result.dataset_handle.id, "INGEST")
        .await;

    // Mimic 3 failures
    for i in 0..3 {
        harness
            .mimic_flow_run_with_outcome(
                &i.to_string(),
                TaskOutcome::Failed(TaskError::empty_recoverable()),
            )
            .await;

        // Sync events explicitly to active re-scheduling
        harness
            .flow_system_event_agent
            .catchup_remaining_events()
            .await
            .unwrap();
    }

    // Must see auto-stopped state with 3 consecutive failures
    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "STOPPED_AUTO".to_string(),
            consecutive_failures: 3,
            maybe_auto_stop_reason: Some("STOP_POLICY".to_string()),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_reach_auto_stop_via_unrecoverable_failure() {
    let harness = FlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Configure trigger
    harness
        .set_time_delta_trigger(&schema, &foo_result.dataset_handle.id, "INGEST")
        .await;

    // Mimic an unrecoverable failure
    harness
        .mimic_flow_run_with_outcome("0", TaskOutcome::Failed(TaskError::empty_unrecoverable()))
        .await;

    // Must see auto-stopped state with unrecoverable failure
    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "STOPPED_AUTO".to_string(),
            consecutive_failures: 1,
            maybe_auto_stop_reason: Some("UNRECOVERABLE_FAILURE".to_string()),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct FlowProcessesHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    flow_system_event_agent: Arc<dyn FlowSystemEventAgent>,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl FlowProcessesHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            let dataset_changes_mock = MockDatasetIncrementQueryService::with_increment_between(
                DatasetIntervalIncrement {
                    num_blocks: 1,
                    num_records: 10,
                    updated_watermark: None,
                },
            );

            b.add::<MetadataQueryServiceImpl>()
                .add_value(dataset_changes_mock)
                .bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>()
                .add_value(FlowSystemEventAgentConfig::local_default())
                .add_value(FlowAgentConfig::test_default())
                .add::<InMemoryFlowProcessState>()
                .add::<InMemoryFlowTriggerEventStore>()
                .add::<InMemoryFlowConfigurationEventStore>()
                .add::<InMemoryFlowEventStore>()
                .add::<InMemoryFlowSystemEventBridge>()
                .add::<TaskSchedulerImpl>()
                .add::<InMemoryTaskEventStore>()
                .add::<FakeDependencyGraphIndexer>()
                .add::<TransformRequestPlannerImpl>();

            kamu_flow_system_services::register_dependencies(&mut b);
            kamu_adapter_flow_dataset::register_dependencies(&mut b, Default::default());

            register_message_dispatcher::<TaskProgressMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_TASK_AGENT,
            );

            register_message_dispatcher::<FlowTriggerUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
            );

            b.build()
        };

        // Init dataset with no sources
        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&catalog_base, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
            flow_system_event_agent: catalog_anonymous.get_one().unwrap(),
            catalog_anonymous,
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
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await
            .unwrap();

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
                                        afterConsecutiveFailures: { maxFailures: 3 }
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

    async fn mimic_flow_run_with_outcome(
        &self,
        flow_id: &str,
        task_outcome: TaskOutcome,
    ) -> TaskID {
        let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
        let flow_task_id = self.mimic_flow_scheduled(flow_id, schedule_time).await;
        let flow_task_metadata = TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, flow_id)]);

        let running_time = schedule_time.duration_round(Duration::seconds(1)).unwrap();
        self.mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
            .await;

        let complete_time = schedule_time.duration_round(Duration::seconds(1)).unwrap();
        self.mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            complete_time,
            task_outcome,
        )
        .await;

        flow_task_id
    }

    async fn mimic_flow_scheduled(&self, flow_id: &str, schedule_time: DateTime<Utc>) -> TaskID {
        let flow_service_test_driver = self
            .catalog_authorized
            .get_one::<dyn FlowAgentTestDriver>()
            .unwrap();

        let flow_id = FlowID::new(flow_id.parse::<u64>().unwrap());
        flow_service_test_driver
            .mimic_flow_scheduled(&self.catalog_authorized, flow_id, schedule_time)
            .await
            .unwrap()
    }

    async fn mimic_task_running(
        &self,
        task_id: TaskID,
        task_metadata: TaskMetadata,
        event_time: DateTime<Utc>,
    ) {
        let task_event_store = self
            .catalog_anonymous
            .get_one::<dyn TaskEventStore>()
            .unwrap();

        let mut task = Task::load(task_id, task_event_store.as_ref())
            .await
            .unwrap();
        task.run(event_time).unwrap();
        task.save(task_event_store.as_ref()).await.unwrap();

        let outbox = self.catalog_authorized.get_one::<dyn Outbox>().unwrap();
        outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_TASK_AGENT,
                TaskProgressMessage::running(event_time, task_id, task_metadata),
            )
            .await
            .unwrap();
    }

    async fn mimic_task_completed(
        &self,
        task_id: TaskID,
        task_metadata: TaskMetadata,
        event_time: DateTime<Utc>,
        task_outcome: TaskOutcome,
    ) {
        let task_event_store = self
            .catalog_anonymous
            .get_one::<dyn TaskEventStore>()
            .unwrap();

        let mut task = Task::load(task_id, task_event_store.as_ref())
            .await
            .unwrap();
        task.finish(event_time, task_outcome.clone()).unwrap();
        task.save(task_event_store.as_ref()).await.unwrap();

        let outbox = self.catalog_authorized.get_one::<dyn Outbox>().unwrap();
        outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_TASK_AGENT,
                TaskProgressMessage::finished(event_time, task_id, task_metadata, task_outcome),
            )
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

#[derive(Debug, Eq, PartialEq)]
struct FlowProcessSummaryBasic {
    flow_type: String,
    effective_state: String,
    consecutive_failures: i32,
    maybe_auto_stop_reason: Option<String>,
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

        let maybe_auto_stop_reason = summary["autoStoppedReason"].as_str();

        Self {
            flow_type,
            effective_state,
            consecutive_failures,
            maybe_auto_stop_reason: maybe_auto_stop_reason.map(ToString::to_string),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
