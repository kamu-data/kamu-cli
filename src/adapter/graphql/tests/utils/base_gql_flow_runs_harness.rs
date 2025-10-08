// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Duration, DurationRound, Utc};
use kamu::TransformRequestPlannerImpl;
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
use messaging_outbox::{Outbox, OutboxExt, register_message_dispatcher};

use crate::utils::{BaseGQLDatasetHarness, BaseGQLFlowHarness};

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

        kamu_flow_system_services::register_dependencies(&mut b);
        kamu_adapter_flow_dataset::register_dependencies(&mut b, Default::default());

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

        b.build()
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
