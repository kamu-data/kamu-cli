// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::*;
use kamu::testing::MockDatasetChangesService;
use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;
use kamu_core::*;
use kamu_datasets::{
    DatasetDependenciesMessage,
    DatasetEntry,
    DatasetLifecycleMessage,
    MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::testing::FakeDatasetEntryService;
use kamu_datasets_services::DependencyGraphServiceImpl;
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::*;
use kamu_task_system::{TaskProgressMessage, MESSAGE_PRODUCER_KAMU_TASK_AGENT};
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::TaskSchedulerImpl;
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxExt, OutboxImmediateImpl};
use time_source::{FakeSystemTimeSource, SystemTimeSource};
use tokio::task::yield_now;

use super::{
    FlowSystemTestListener,
    ManualFlowAbortArgs,
    ManualFlowAbortDriver,
    ManualFlowTriggerArgs,
    ManualFlowTriggerDriver,
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
    pub mock_dataset_changes: Option<MockDatasetChangesService>,
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
            ))
            .add::<InMemoryFlowEventStore>()
            .add::<InMemoryFlowConfigurationEventStore>()
            .add::<InMemoryFlowTriggerEventStore>()
            .add_value(fake_system_time_source.clone())
            .bind::<dyn SystemTimeSource, FakeSystemTimeSource>()
            .add_value(mock_dataset_changes)
            .bind::<dyn DatasetChangesService, MockDatasetChangesService>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<TaskSchedulerImpl>()
            .add::<InMemoryTaskEventStore>()
            .add::<DatabaseTransactionRunner>()
            .add::<FakeDatasetEntryService>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            kamu_flow_system_services::register_dependencies(&mut b);

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
        let dataset_id = odf::DatasetID::new_generated_ed25519().1;
        let owner_id = odf::metadata::testing::account_id_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );

        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            created_at: self.fake_system_time_source.now(),
            id: dataset_id.clone(),
            owner_id: owner_id.clone(),
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
        let dataset_id = odf::DatasetID::new_generated_ed25519().1;
        let owner_id = odf::metadata::testing::account_id_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );

        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            created_at: self.fake_system_time_source.now(),
            id: dataset_id.clone(),
            owner_id: owner_id.clone(),
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

    pub async fn set_dataset_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
        trigger_rule: FlowTriggerRule,
    ) {
        self.flow_trigger_service
            .set_trigger(
                request_time,
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                trigger_rule,
            )
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_ingest(
        &self,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
        ingest_rule: IngestRule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                FlowConfigurationRule::IngestRule(ingest_rule),
            )
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_reset_rule(
        &self,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
        reset_rule: ResetRule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                FlowConfigurationRule::ResetRule(reset_rule),
            )
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_compaction_rule(
        &self,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
        compaction_rule: CompactionRule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                FlowConfigurationRule::CompactionRule(compaction_rule),
            )
            .await
            .unwrap();
    }

    pub async fn pause_dataset_flow(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let current_trigger = self
            .flow_trigger_service
            .find_trigger(flow_key.clone())
            .await
            .unwrap()
            .unwrap();

        self.flow_trigger_service
            .set_trigger(request_time, flow_key, true, current_trigger.rule)
            .await
            .unwrap();
    }

    pub async fn resume_dataset_flow(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let current_trigger = self
            .flow_trigger_service
            .find_trigger(flow_key.clone())
            .await
            .unwrap()
            .unwrap();

        self.flow_trigger_service
            .set_trigger(request_time, flow_key, false, current_trigger.rule)
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
        args: ManualFlowTriggerArgs,
    ) -> ManualFlowTriggerDriver {
        ManualFlowTriggerDriver::new(self.catalog.clone(), self.catalog.get_one().unwrap(), args)
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
