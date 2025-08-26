// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::*;
use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;
use kamu_adapter_flow_dataset::*;
use kamu_datasets::*;
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::DependencyGraphServiceImpl;
use kamu_datasets_services::testing::{FakeDatasetEntryService, MockDatasetIncrementQueryService};
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::*;
use kamu_task_system::{MESSAGE_PRODUCER_KAMU_TASK_AGENT, TaskProgressMessage};
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::TaskSchedulerImpl;
use messaging_outbox::{Outbox, OutboxExt, OutboxImmediateImpl, register_message_dispatcher};
use time_source::{FakeSystemTimeSource, SystemTimeSource};
use tokio::task::yield_now;

use super::{
    FlowSystemTestListener,
    ManualFlowAbortArgs,
    ManualFlowAbortDriver,
    ManualFlowActivationArgs,
    ManualFlowActivationDriver,
    TaskDriver,
    TaskDriverArgs,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const SCHEDULING_ALIGNMENT_MS: i64 = 10;
pub(crate) const SCHEDULING_MANDATORY_THROTTLING_PERIOD_MS: i64 = SCHEDULING_ALIGNMENT_MS * 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowHarness {
    pub catalog: Catalog,
    pub outbox: Arc<dyn Outbox>,
    pub fake_dataset_entry_service: Arc<FakeDatasetEntryService>,
    pub fake_system_time_source: FakeSystemTimeSource,
    pub dataset_entry_service: Arc<dyn DatasetEntryService>,

    pub flow_configuration_service: Arc<dyn FlowConfigurationService>,
    pub flow_trigger_service: Arc<dyn FlowTriggerService>,
    pub flow_trigger_event_store: Arc<dyn FlowTriggerEventStore>,
    pub flow_agent: Arc<FlowAgentImpl>,
    pub flow_query_service: Arc<dyn FlowQueryService>,
    pub flow_event_store: Arc<dyn FlowEventStore>,
}

#[derive(Default)]
pub(crate) struct FlowHarnessOverrides {
    pub awaiting_step: Option<Duration>,
    pub mandatory_throttling_period: Option<Duration>,
    pub mock_dataset_changes: Option<MockDatasetIncrementQueryService>,
    pub mock_transform_flow_evaluator: Option<MockTransformFlowEvaluator>,
}

impl FlowHarness {
    pub fn new() -> Self {
        Self::with_overrides(FlowHarnessOverrides::default())
    }

    pub fn with_overrides(overrides: FlowHarnessOverrides) -> Self {
        let t = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();
        let fake_system_time_source = FakeSystemTimeSource::new(t);

        let awaiting_step = overrides
            .awaiting_step
            .unwrap_or(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS));

        let mandatory_throttling_period =
            overrides
                .mandatory_throttling_period
                .unwrap_or(Duration::milliseconds(
                    SCHEDULING_MANDATORY_THROTTLING_PERIOD_MS,
                ));

        let mock_dataset_changes = overrides.mock_dataset_changes.unwrap_or_default();
        let mock_transform_flow_evaluator =
            overrides.mock_transform_flow_evaluator.unwrap_or_default();

        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<FlowSystemTestListener>()
            .add_value(FlowAgentConfig::new(
                awaiting_step,
                mandatory_throttling_period,
                HashMap::new(),
            ))
            .add::<InMemoryFlowEventStore>()
            .add::<InMemoryFlowConfigurationEventStore>()
            .add::<InMemoryFlowTriggerEventStore>()
            .add_value(fake_system_time_source.clone())
            .bind::<dyn SystemTimeSource, FakeSystemTimeSource>()
            .add_value(mock_dataset_changes)
            .bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>()
            .add_value(mock_transform_flow_evaluator)
            .bind::<dyn TransformFlowEvaluator, MockTransformFlowEvaluator>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<TaskSchedulerImpl>()
            .add::<InMemoryTaskEventStore>()
            .add::<DatabaseTransactionRunner>()
            .add::<FakeDatasetEntryService>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            kamu_flow_system_services::register_dependencies(&mut b);
            kamu_adapter_flow_dataset::register_dependencies(
                &mut b,
                kamu_adapter_flow_dataset::FlowDatasetAdapterDependencyOpts {
                    with_default_transform_evaluator: false,
                },
            );

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );
            register_message_dispatcher::<DatasetDependenciesMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
            );
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
            register_message_dispatcher::<FlowAgentUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_AGENT,
            );
            register_message_dispatcher::<FlowProgressMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
            );

            b.build()
        };

        Self {
            outbox: catalog.get_one().unwrap(),
            fake_dataset_entry_service: catalog.get_one().unwrap(),
            dataset_entry_service: catalog.get_one().unwrap(),

            flow_agent: catalog.get_one().unwrap(),
            flow_query_service: catalog.get_one().unwrap(),
            flow_configuration_service: catalog.get_one().unwrap(),
            flow_trigger_service: catalog.get_one().unwrap(),
            flow_trigger_event_store: catalog.get_one().unwrap(),
            flow_event_store: catalog.get_one().unwrap(),
            fake_system_time_source,
            catalog,
        }
    }

    pub async fn create_root_dataset(&self, dataset_alias: odf::DatasetAlias) -> odf::DatasetID {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(dataset_alias.dataset_name.as_bytes());
        let owner_id = odf::metadata::testing::account_id_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );
        let owner_name = odf::metadata::testing::account_name_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );

        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            created_at: self.fake_system_time_source.now(),
            id: dataset_id.clone(),
            owner_id: owner_id.clone(),
            owner_name,
            name: dataset_alias.dataset_name.clone(),
            kind: odf::DatasetKind::Root,
        });

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    dataset_id.clone(),
                    owner_id,
                    odf::DatasetVisibility::Public,
                    dataset_alias.dataset_name,
                ),
            )
            .await
            .unwrap();

        dataset_id
    }

    pub async fn create_derived_dataset(
        &self,
        dataset_alias: odf::DatasetAlias,
        input_ids: Vec<odf::DatasetID>,
    ) -> odf::DatasetID {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(dataset_alias.dataset_name.as_bytes());
        let owner_id = odf::metadata::testing::account_id_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );
        let owner_name = odf::metadata::testing::account_name_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );

        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            created_at: self.fake_system_time_source.now(),
            id: dataset_id.clone(),
            owner_id: owner_id.clone(),
            owner_name,
            name: dataset_alias.dataset_name.clone(),
            kind: odf::DatasetKind::Derivative,
        });

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    dataset_id.clone(),
                    owner_id,
                    odf::DatasetVisibility::Public,
                    dataset_alias.dataset_name,
                ),
            )
            .await
            .unwrap();

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
                DatasetDependenciesMessage::updated(&dataset_id, input_ids, vec![]),
            )
            .await
            .unwrap();

        dataset_id
    }

    pub async fn eager_initialization(&self) {
        use init_on_startup::InitOnStartup;

        self.flow_agent.run_initialization().await.unwrap();
    }

    pub async fn issue_dataset_deleted(&self, dataset_id: &odf::DatasetID) {
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::deleted(dataset_id.clone()),
            )
            .await
            .unwrap();
    }

    pub async fn set_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        trigger_rule: FlowTriggerRule,
        stop_policy: FlowTriggerStopPolicy,
    ) {
        self.flow_trigger_service
            .set_trigger(request_time, flow_binding, trigger_rule, stop_policy)
            .await
            .unwrap();
    }

    pub async fn get_flow_trigger_status(
        &self,
        flow_binding: &FlowBinding,
    ) -> Option<FlowTriggerStatus> {
        self.flow_trigger_service
            .find_trigger(flow_binding)
            .await
            .unwrap()
            .map(|t| t.status)
    }

    pub async fn set_dataset_flow_ingest(
        &self,
        flow_binding: FlowBinding,
        ingest_rule: FlowConfigRuleIngest,
        retry_policy: Option<RetryPolicy>,
    ) {
        self.flow_configuration_service
            .set_configuration(flow_binding, ingest_rule.into_flow_config(), retry_policy)
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_reset_rule(
        &self,
        flow_binding: FlowBinding,
        reset_rule: FlowConfigRuleReset,
    ) {
        self.flow_configuration_service
            .set_configuration(flow_binding, reset_rule.into_flow_config(), None)
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_compaction_rule(
        &self,
        flow_binding: FlowBinding,
        compaction_rule: FlowConfigRuleCompact,
    ) {
        self.flow_configuration_service
            .set_configuration(flow_binding, compaction_rule.into_flow_config(), None)
            .await
            .unwrap();
    }

    pub async fn pause_flow(&self, request_time: DateTime<Utc>, flow_binding: &FlowBinding) {
        self.flow_trigger_service
            .pause_flow_trigger(request_time, flow_binding)
            .await
            .unwrap();
    }

    pub async fn resume_flow(&self, request_time: DateTime<Utc>, flow_binding: &FlowBinding) {
        self.flow_trigger_service
            .resume_flow_trigger(request_time, flow_binding)
            .await
            .unwrap();
    }

    pub fn task_driver(&self, args: TaskDriverArgs) -> TaskDriver {
        TaskDriver::new(
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            args,
        )
    }

    pub fn manual_flow_trigger_driver(
        &self,
        args: ManualFlowActivationArgs,
    ) -> ManualFlowActivationDriver {
        ManualFlowActivationDriver::new(self.catalog.clone(), self.catalog.get_one().unwrap(), args)
    }

    pub fn manual_flow_abort_driver(&self, args: ManualFlowAbortArgs) -> ManualFlowAbortDriver {
        ManualFlowAbortDriver::new(self.catalog.clone(), self.catalog.get_one().unwrap(), args)
    }

    pub fn now_datetime(&self) -> DateTime<Utc> {
        self.fake_system_time_source.now()
    }

    pub async fn advance_time(&self, time_quantum: Duration) {
        self.advance_time_custom_alignment(
            Duration::milliseconds(SCHEDULING_ALIGNMENT_MS),
            time_quantum,
        )
        .await;
    }

    pub async fn advance_time_custom_alignment(&self, alignment: Duration, time_quantum: Duration) {
        // Examples:
        // 12 ÷ 4 = 3
        // 15 ÷ 4 = 4
        // 16 ÷ 4 = 4
        fn div_up(a: i64, b: i64) -> i64 {
            (a + (b - 1)) / b
        }

        let time_increments_count = div_up(
            time_quantum.num_milliseconds(),
            alignment.num_milliseconds(),
        );

        for _ in 0..time_increments_count {
            yield_now().await;
            self.fake_system_time_source.advance(alignment);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
