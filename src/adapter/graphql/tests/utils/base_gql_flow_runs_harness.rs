// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use async_graphql::value;
use chrono::{DateTime, Duration, DurationRound, Utc};
use indoc::indoc;
use kamu::TransformRequestPlannerImpl;
use kamu_adapter_flow_dataset::*;
use kamu_adapter_flow_webhook::webhook_deliver_binding;
use kamu_core::TenancyConfig;
use kamu_datasets::DatasetIncrementQueryService;
use kamu_datasets_services::testing::{
    FakeDependencyGraphIndexer,
    MockDatasetIncrementQueryService,
};
use kamu_flow_system::*;
use kamu_task_system::*;
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::TaskSchedulerImpl;
use kamu_webhooks::*;
use kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore;
use messaging_outbox::{Outbox, OutboxExt, register_message_dispatcher};

use crate::utils::{BaseGQLDatasetHarness, BaseGQLFlowHarness, GraphQLQueryRequest};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLFlowHarness, base_gql_flow_harness)]
pub struct BaseGQLFlowRunsHarness {
    base_gql_flow_harness: BaseGQLFlowHarness,
}

#[derive(Default)]
pub struct FlowRunsHarnessOverrides {
    pub dataset_changes_mock: Option<MockDatasetIncrementQueryService>,
}

impl BaseGQLFlowRunsHarness {
    pub async fn new(base_gql_harness: BaseGQLDatasetHarness, runs_catalog: dill::Catalog) -> Self {
        let base_gql_flow_harness = BaseGQLFlowHarness::new(base_gql_harness, runs_catalog).await;

        Self::run_related_startup_jobs(&base_gql_flow_harness.catalog_authorized).await;

        Self {
            base_gql_flow_harness,
        }
    }

    pub async fn with_overrides(overrides: FlowRunsHarnessOverrides) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let base_gql_flow_catalog =
            BaseGQLFlowHarness::make_base_gql_flow_catalog(base_gql_harness.catalog());

        let base_gql_flow_runs_catalog =
            Self::make_base_gql_flow_runs_catalog(&base_gql_flow_catalog, overrides);

        Self::new(base_gql_harness, base_gql_flow_runs_catalog).await
    }

    pub fn make_base_gql_flow_runs_catalog(
        base_catalog: &dill::Catalog,
        overrides: FlowRunsHarnessOverrides,
    ) -> dill::Catalog {
        let mut b = dill::CatalogBuilder::new_chained(base_catalog);

        let dataset_changes_mock = overrides.dataset_changes_mock.unwrap_or_default();

        b.add_value(dataset_changes_mock)
            .bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>()
            .add_value(FlowAgentConfig::test_default())
            .add_value(FlowSystemEventAgentConfig::local_default())
            .add::<TaskSchedulerImpl>()
            .add::<InMemoryTaskEventStore>()
            .add::<TransformRequestPlannerImpl>()
            .add::<FakeDependencyGraphIndexer>();

        b.add::<InMemoryWebhookSubscriptionEventStore>()
            .add_value(WebhooksConfig::default());

        kamu_flow_system_services::register_dependencies(&mut b);
        kamu_webhooks_services::register_dependencies(&mut b);

        kamu_adapter_flow_dataset::register_dependencies(&mut b, Default::default());
        kamu_adapter_flow_webhook::register_dependencies(&mut b);

        register_message_dispatcher::<TaskProgressMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_TASK_AGENT,
        );

        register_message_dispatcher::<FlowConfigurationUpdatedMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
        );

        register_message_dispatcher::<FlowTriggerUpdatedMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
        );

        register_message_dispatcher::<WebhookSubscriptionLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE,
        );
        register_message_dispatcher::<WebhookSubscriptionEventChangesMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_SERVICE,
        );

        b.build()
    }

    async fn run_related_startup_jobs(catalog: &dill::Catalog) {
        init_on_startup::run_startup_jobs_ex(
            catalog,
            init_on_startup::RunStartupJobsOptions {
                job_selector: Some(init_on_startup::JobSelector::AllOf(HashSet::from([
                    JOB_KAMU_TASKS_AGENT_RECOVERY,
                    JOB_KAMU_FLOW_AGENT_RECOVERY,
                ]))),
            },
        )
        .await
        .unwrap();
    }

    pub fn trigger_ingest_flow_mutation(&self, id: &odf::DatasetID) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerIngestFlow {
                                    __typename,
                                    message
                                    ... on TriggerFlowSuccess {
                                        flow {
                                            __typename
                                            flowId
                                            status
                                            outcome {
                                                ...on FlowSuccessResult {
                                                    message
                                                }
                                                ...on FlowAbortedResult {
                                                    message
                                                }
                                                ...on FlowFailedError {
                                                    reason {
                                                        ...on TaskFailureReasonGeneral {
                                                            message
                                                            recoverable
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
            }
            "#
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string()
            })),
        )
    }

    pub fn trigger_transform_flow_mutation(&self, id: &odf::DatasetID) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerTransformFlow {
                                    __typename,
                                    message
                                    ... on TriggerFlowSuccess {
                                        flow {
                                            __typename
                                            flowId
                                            status
                                            outcome {
                                                ...on FlowSuccessResult {
                                                    message
                                                }
                                                ...on FlowAbortedResult {
                                                    message
                                                }
                                                ...on FlowFailedError {
                                                    reason {
                                                        ...on TaskFailureReasonGeneral {
                                                            message
                                                            recoverable
                                                        }
                                                        ...on TaskFailureReasonInputDatasetCompacted {
                                                            message
                                                            inputDataset {
                                                                id
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
                }
            }
            "#
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string()
            })),
        )
    }

    pub fn trigger_reset_flow_mutation(
        &self,
        id: &odf::DatasetID,
        new_head_hash: &odf::Multihash,
        old_head_hash: &odf::Multihash,
    ) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!, $newHeadHash: String!, $oldHeadHash: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerResetFlow (
                                    resetConfigInput: {
                                        mode: {
                                            custom: {
                                                newHeadHash: $newHeadHash
                                            }
                                        },
                                        oldHeadHash: $oldHeadHash,
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on TriggerFlowSuccess {
                                        flow {
                                            __typename
                                            flowId
                                            status
                                            outcome {
                                                ...on FlowSuccessResult {
                                                    message
                                                }
                                                ...on FlowAbortedResult {
                                                    message
                                                }
                                                ...on FlowFailedError {
                                                    reason {
                                                        ...on TaskFailureReasonGeneral {
                                                            message
                                                            recoverable
                                                        }
                                                        ...on TaskFailureReasonInputDatasetCompacted {
                                                            message
                                                            inputDataset {
                                                                id
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
                }
            }
            "#
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string(),
                "newHeadHash": new_head_hash.to_string(),
                "oldHeadHash": old_head_hash.to_string()
            })),
        )
    }

    pub fn trigger_compaction_flow_mutation(&self, id: &odf::DatasetID) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerCompactionFlow {
                                    __typename,
                                    message
                                    ... on TriggerFlowSuccess {
                                        flow {
                                            __typename
                                            flowId
                                            status
                                            outcome {
                                                ...on FlowSuccessResult {
                                                    message
                                                }
                                                ...on FlowAbortedResult {
                                                    message
                                                }
                                                ...on FlowFailedError {
                                                    reason {
                                                        ...on TaskFailureReasonGeneral {
                                                            message
                                                            recoverable
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
            }
            "#
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string()
            })),
        )
    }

    pub fn trigger_compaction_flow_mutation_with_config(
        &self,
        id: &odf::DatasetID,
        max_slice_records: u64,
        max_slice_size: u64,
    ) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!, $maxSliceRecords: Int!, $maxSliceSize: Int!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerCompactionFlow (
                                    compactionConfigInput: {
                                        maxSliceRecords: $maxSliceRecords,
                                        maxSliceSize: $maxSliceSize,
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on TriggerFlowSuccess {
                                        flow {
                                            __typename
                                            flowId
                                            status
                                            outcome {
                                                ...on FlowSuccessResult {
                                                    message
                                                }
                                                ...on FlowAbortedResult {
                                                    message
                                                }
                                                ...on FlowFailedError {
                                                    reason {
                                                        ...on TaskFailureReasonGeneral {
                                                            message
                                                            recoverable
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
            }
            "#
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string(),
                "maxSliceRecords": max_slice_records,
                "maxSliceSize": max_slice_size
            })),
        )
    }

    pub fn trigger_reset_to_metadata_flow_mutation(
        &self,
        id: &odf::DatasetID,
    ) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerResetToMetadataFlow {
                                    __typename,
                                    message
                                    ... on TriggerFlowSuccess {
                                        flow {
                                            __typename
                                            flowId
                                            status
                                            outcome {
                                                ...on FlowSuccessResult {
                                                    message
                                                }
                                                ...on FlowAbortedResult {
                                                    message
                                                }
                                                ...on FlowFailedError {
                                                    reason {
                                                        ...on TaskFailureReasonGeneral {
                                                            message
                                                            recoverable
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
            }
            "#
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string()
            })),
        )
    }

    pub async fn create_webhook_for_dataset_updates(
        &self,
        dataset_id: &odf::DatasetID,
        webhook_label: &str,
    ) -> WebhookSubscriptionID {
        let create_webhook_uc = self
            .catalog_authorized
            .get_one::<dyn CreateWebhookSubscriptionUseCase>()
            .unwrap();

        let result = create_webhook_uc
            .execute(
                Some(dataset_id.clone()),
                url::Url::parse(&format!("https://example.com/{webhook_label}")).unwrap(),
                vec![WebhookEventTypeCatalog::dataset_ref_updated()],
                WebhookSubscriptionLabel::try_new(webhook_label.to_string()).unwrap(),
            )
            .await
            .unwrap();

        result.subscription_id
    }

    pub async fn pause_webhook_subscription(&self, subscription_id: WebhookSubscriptionID) {
        let pause_webhook_uc = self
            .catalog_authorized
            .get_one::<dyn PauseWebhookSubscriptionUseCase>()
            .unwrap();

        let subscription_store = self
            .catalog_authorized
            .get_one::<dyn WebhookSubscriptionEventStore>()
            .unwrap();

        let mut subscription =
            WebhookSubscription::load(subscription_id, subscription_store.as_ref())
                .await
                .unwrap();

        pause_webhook_uc.execute(&mut subscription).await.unwrap();
    }

    pub async fn mimic_webhook_flow_success(
        &self,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
    ) {
        let flow_id = self.trigger_webhook_flow(dataset_id, subscription_id).await;

        self.mimic_flow_run_with_outcome(
            flow_id.to_string().as_str(),
            TaskOutcome::Success(TaskResult::empty()),
        )
        .await;
    }

    pub async fn mimic_webhook_flow_failure(
        &self,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
        unrecoverable: bool,
    ) {
        let flow_id = self.trigger_webhook_flow(dataset_id, subscription_id).await;

        self.mimic_flow_run_with_outcome(
            flow_id.to_string().as_str(),
            TaskOutcome::Failed(if unrecoverable {
                TaskError::empty_unrecoverable()
            } else {
                TaskError::empty_recoverable()
            }),
        )
        .await;
    }

    pub async fn trigger_webhook_flow(
        &self,
        dataset_id: &odf::DatasetID,
        subscription_id: WebhookSubscriptionID,
    ) -> FlowID {
        let flow_run_service = self
            .catalog_authorized
            .get_one::<dyn FlowRunService>()
            .unwrap();

        let flow_binding = webhook_deliver_binding(
            subscription_id,
            &WebhookEventTypeCatalog::dataset_ref_updated(),
            Some(dataset_id),
        );

        let flow_state = flow_run_service
            .run_flow_automatically(
                Utc::now(),
                &flow_binding,
                vec![FlowActivationCause::ResourceUpdate(
                    FlowActivationCauseResourceUpdate {
                        activation_time: Utc::now(),
                        resource_type: DATASET_RESOURCE_TYPE.to_string(),
                        changes: ResourceChanges::NewData(ResourceDataChanges {
                            blocks_added: 1,
                            records_added: 10,
                            new_watermark: None,
                        }),
                        details: serde_json::to_value(DatasetResourceUpdateDetails {
                            dataset_id: dataset_id.clone(),
                            source: DatasetUpdateSource::UpstreamFlow {
                                flow_type: FLOW_TYPE_DATASET_INGEST.to_string(),
                                flow_id: FlowID::new(1),
                                maybe_flow_config_snapshot: None,
                            },
                            new_head: odf::Multihash::from_digest_sha3_256(b"new_head"),
                            old_head_maybe: Some(odf::Multihash::from_digest_sha3_256(b"old_head")),
                        })
                        .unwrap(),
                    },
                )],
                Some(FlowTriggerRule::Reactive(ReactiveRule::empty())),
                None,
            )
            .await
            .unwrap();

        flow_state.flow_id
    }

    pub async fn mimic_flow_run_with_outcome(
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

    pub async fn mimic_flow_scheduled(
        &self,
        flow_id: &str,
        schedule_time: DateTime<Utc>,
    ) -> TaskID {
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

    pub async fn mimic_flow_secondary_activation_cause(
        &self,
        flow_id: &str,
        activation_cause: FlowActivationCause,
    ) {
        let flow_event_store = self
            .catalog_authorized
            .get_one::<dyn FlowEventStore>()
            .unwrap();

        let mut flow = Flow::load(
            FlowID::new(flow_id.parse::<u64>().unwrap()),
            flow_event_store.as_ref(),
        )
        .await
        .unwrap();

        flow.add_activation_cause_if_unique(Utc::now(), activation_cause)
            .unwrap();
        flow.save(flow_event_store.as_ref()).await.unwrap();
    }

    pub async fn mimic_task_running(
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

    pub async fn mimic_task_completed(
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
