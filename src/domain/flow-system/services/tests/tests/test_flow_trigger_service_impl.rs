// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, Utc};
use database_common_macros::transactional_method1;
use dill::*;
use futures::TryStreamExt;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::DependencyGraphServiceImpl;
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::*;
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxImmediateImpl};
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSourceDefault;

use super::FlowTriggerTestListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_visibility() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;

    let foo_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    harness
        .set_dataset_flow_trigger(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_trigger.clone(),
        )
        .await;

    let foo_compaction_trigger = FlowTriggerRule::Schedule(Duration::days(1).into());
    harness
        .set_dataset_flow_trigger(
            foo_id.clone(),
            DatasetFlowType::HardCompaction,
            foo_compaction_trigger.clone(),
        )
        .await;

    let bar_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    harness
        .set_dataset_flow_trigger(
            bar_id.clone(),
            DatasetFlowType::Ingest,
            bar_ingest_trigger.clone(),
        )
        .await;

    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(3, triggers.len());

    for (dataset_id, dataset_flow_type, trigger) in [
        (foo_id.clone(), DatasetFlowType::Ingest, &foo_ingest_trigger),
        (
            foo_id,
            DatasetFlowType::HardCompaction,
            &foo_compaction_trigger,
        ),
        (bar_id, DatasetFlowType::Ingest, &bar_ingest_trigger),
    ] {
        harness.expect_dataset_flow_trigger(&triggers, dataset_id, dataset_flow_type, trigger);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_individual_dataset_flows() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());
    assert_eq!(0, harness.trigger_events_count());

    // Make a dataset and configure daily ingestion schedule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    harness
        .set_dataset_flow_trigger(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_trigger.clone(),
        )
        .await;

    // It should be visible in the list of enabled triggers and produced 1 trigger
    // event
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(1, triggers.len());
    harness.expect_dataset_flow_trigger(
        &triggers,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_trigger,
    );
    assert_eq!(1, harness.trigger_events_count());

    // Now, pause this flow trigger
    harness
        .pause_dataset_flow(foo_id.clone(), DatasetFlowType::Ingest)
        .await;

    // It should disappear from the list of enabled triggers, and produce 1 more
    // event
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(0, triggers.len());
    assert_eq!(2, harness.trigger_events_count());

    // Still, we should see it's state as paused in the repository directly
    let flow_trigger_state = harness
        .get_dataset_flow_trigger_from_store(foo_id.clone(), DatasetFlowType::Ingest)
        .await;
    assert_eq!(
        flow_trigger_state.status,
        FlowTriggerStatus::PausedTemporarily
    );
    assert_eq!(flow_trigger_state.rule, foo_ingest_trigger.clone());

    // Now, resume the trigger
    harness
        .resume_dataset_flow(foo_id.clone(), DatasetFlowType::Ingest)
        .await;

    // It should be visible in the list of active triggers again, and produce
    // another event yet
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(1, triggers.len());
    harness.expect_dataset_flow_trigger(
        &triggers,
        foo_id,
        DatasetFlowType::Ingest,
        &foo_ingest_trigger,
    );
    assert_eq!(3, harness.trigger_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_all_dataset_flows() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());
    assert_eq!(0, harness.trigger_events_count());

    // Make a dataset and configure ingestion and compaction schedule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    harness
        .set_dataset_flow_trigger(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_trigger.clone(),
        )
        .await;
    let foo_compaction_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    harness
        .set_dataset_flow_trigger(
            foo_id.clone(),
            DatasetFlowType::HardCompaction,
            foo_compaction_trigger.clone(),
        )
        .await;

    // Both should be visible in the list of enabled triggers, 2 events expected
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(2, triggers.len());
    harness.expect_dataset_flow_trigger(
        &triggers,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_trigger,
    );
    harness.expect_dataset_flow_trigger(
        &triggers,
        foo_id.clone(),
        DatasetFlowType::HardCompaction,
        &foo_compaction_trigger,
    );
    assert_eq!(2, harness.trigger_events_count());

    // Now, pause all flows of this dataset
    harness.pause_all_dataset_flows(foo_id.clone()).await;

    // Both should disappear from the list of enabled triggers,
    // and both should produce events
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(0, triggers.len());
    assert_eq!(4, harness.trigger_events_count());

    // Still, we should see their state as paused in the repository directly

    let flow_trigger_ingest_state = harness
        .get_dataset_flow_trigger_from_store(foo_id.clone(), DatasetFlowType::Ingest)
        .await;
    assert_eq!(
        flow_trigger_ingest_state.status,
        FlowTriggerStatus::PausedTemporarily
    );
    assert_eq!(flow_trigger_ingest_state.rule, foo_ingest_trigger.clone());

    let flow_trigger_compaction_state = harness
        .get_dataset_flow_trigger_from_store(foo_id.clone(), DatasetFlowType::HardCompaction)
        .await;
    assert_eq!(
        flow_trigger_compaction_state.status,
        FlowTriggerStatus::PausedTemporarily
    );
    assert_eq!(
        flow_trigger_compaction_state.rule,
        foo_compaction_trigger.clone()
    );

    // Now, resume all triggers
    harness.resume_all_dataset_flows(foo_id.clone()).await;

    // They should be visible in the list of active triggers again, and again,we
    // should get 2 extra events
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(2, triggers.len());
    harness.expect_dataset_flow_trigger(
        &triggers,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_trigger,
    );
    harness.expect_dataset_flow_trigger(
        &triggers,
        foo_id.clone(),
        DatasetFlowType::HardCompaction,
        &foo_compaction_trigger,
    );
    assert_eq!(6, harness.trigger_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_individual_system_flows() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());
    assert_eq!(0, harness.trigger_events_count());

    // Configure GC schedule
    let gc_schedule: Schedule = Duration::minutes(30).into();
    harness
        .set_system_flow_schedule(SystemFlowType::GC, gc_schedule.clone())
        .await
        .unwrap();

    // It should be visible in the list of enabled triggers, and create 1 event
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(1, triggers.len());
    harness.expect_system_flow_schedule(&triggers, SystemFlowType::GC, &gc_schedule);
    assert_eq!(1, harness.trigger_events_count());

    // Now, pause this flow trigger
    harness.pause_system_flow(SystemFlowType::GC).await;

    // It should disappear from the list of enabled triggers, and create 1 more
    // event
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(0, triggers.len());
    assert_eq!(2, harness.trigger_events_count());

    // Still, we should see it's state as paused in the repository directly
    let flow_trigger_state = harness
        .get_system_flow_trigger_from_store(SystemFlowType::GC)
        .await;
    assert_eq!(
        flow_trigger_state.status,
        FlowTriggerStatus::PausedTemporarily
    );
    assert_eq!(
        flow_trigger_state.rule,
        FlowTriggerRule::Schedule(gc_schedule.clone())
    );

    // Now, resume the triggers
    harness.resume_system_flow(SystemFlowType::GC).await;

    // It should be visible in the list of active triggers again, and create
    // another event yet
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(1, triggers.len());
    harness.expect_system_flow_schedule(&triggers, SystemFlowType::GC, &gc_schedule);
    assert_eq!(3, harness.trigger_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());

    // Make a dataset and configure ingest trigger
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    harness
        .set_dataset_flow_trigger(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_trigger.clone(),
        )
        .await;

    // It should be visible in the list of triggers
    let configs = harness.list_enabled_triggers().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_trigger(
        &configs,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_trigger,
    );

    // Now, delete the dataset
    harness.delete_dataset(&foo_id).await;

    // The dataset should not be visible in the list of configs
    let configs = harness.list_enabled_triggers().await;
    assert_eq!(0, configs.len());

    // Still, we should see it's state as permanently stopped in the repository
    let flow_config_state = harness
        .get_dataset_flow_trigger_from_store(foo_id, DatasetFlowType::Ingest)
        .await;
    assert_eq!(flow_config_state.rule, foo_ingest_trigger);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowTriggerHarness {
    _tmp_dir: tempfile::TempDir,
    catalog: Catalog,
    flow_trigger_service: Arc<dyn FlowTriggerService>,
    flow_trigger_event_store: Arc<dyn FlowTriggerEventStore>,
    trigger_listener: Arc<FlowTriggerTestListener>,
}

impl FlowTriggerHarness {
    fn new() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = tmp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<DidGeneratorDefault>()
            .add::<FlowTriggerTestListener>()
            .add::<FlowTriggerServiceImpl>()
            .add::<InMemoryFlowTriggerEventStore>()
            .add::<SystemTimeSourceDefault>()
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
            .bind::<dyn DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
            .add::<DatasetRegistrySoloUnitBridge>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<DeleteDatasetUseCaseImpl>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
            );
            register_message_dispatcher::<FlowTriggerUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
            );

            b.build()
        };

        let flow_trigger_service = catalog.get_one::<dyn FlowTriggerService>().unwrap();
        let flow_trigger_event_store = catalog.get_one::<dyn FlowTriggerEventStore>().unwrap();
        let flow_trigger_events_listener = catalog.get_one::<FlowTriggerTestListener>().unwrap();

        Self {
            _tmp_dir: tmp_dir,
            catalog,
            flow_trigger_service,
            flow_trigger_event_store,
            trigger_listener: flow_trigger_events_listener,
        }
    }

    async fn list_enabled_triggers(&self) -> HashMap<FlowKey, FlowTriggerState> {
        let active_triggers: Vec<_> = self
            .flow_trigger_service
            .list_enabled_triggers()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut res = HashMap::new();
        for active_trigger in active_triggers {
            res.insert(active_trigger.flow_key.clone(), active_trigger);
        }
        res
    }

    #[transactional_method1(flow_trigger_service: Arc<dyn
    FlowTriggerService>)]
    async fn set_system_flow_schedule(
        &self,
        system_flow_type: SystemFlowType,
        schedule: Schedule,
    ) -> Result<(), SetFlowTriggerError> {
        flow_trigger_service
            .set_trigger(
                Utc::now(),
                system_flow_type.into(),
                false,
                FlowTriggerRule::Schedule(schedule),
            )
            .await?;
        Ok(())
    }

    async fn set_dataset_flow_trigger(
        &self,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
        trigger_rule: FlowTriggerRule,
    ) {
        self.flow_trigger_service
            .set_trigger(
                Utc::now(),
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                trigger_rule,
            )
            .await
            .unwrap();
    }

    fn expect_dataset_flow_trigger(
        &self,
        triggers: &HashMap<FlowKey, FlowTriggerState>,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
        expected_rule: &FlowTriggerRule,
    ) {
        assert_matches!(
            triggers.get(&(FlowKeyDataset::new(dataset_id, dataset_flow_type).into())),
            Some(FlowTriggerState {
                status: FlowTriggerStatus::Active,
                rule: actual_rule,
                ..
            }) if actual_rule == expected_rule
        );
    }

    fn expect_system_flow_schedule(
        &self,
        enabled_triggers: &HashMap<FlowKey, FlowTriggerState>,
        system_flow_type: SystemFlowType,
        expected_schedule: &Schedule,
    ) {
        assert_matches!(
            enabled_triggers.get(&(system_flow_type.into())),
            Some(FlowTriggerState {
                status: FlowTriggerStatus::Active,
                rule: FlowTriggerRule::Schedule(actual_schedule),
                ..
            }) if actual_schedule == expected_schedule
        );
    }

    async fn get_dataset_flow_trigger_from_store(
        &self,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) -> FlowTriggerState {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let flow_trigger = FlowTrigger::load(flow_key, self.flow_trigger_event_store.as_ref())
            .await
            .unwrap();
        flow_trigger.into()
    }

    async fn get_system_flow_trigger_from_store(
        &self,
        system_flow_type: SystemFlowType,
    ) -> FlowTriggerState {
        let flow_key: FlowKey = FlowKey::System(FlowKeySystem::new(system_flow_type));
        let flow_trigger = FlowTrigger::load(flow_key, self.flow_trigger_event_store.as_ref())
            .await
            .unwrap();
        flow_trigger.into()
    }

    async fn create_root_dataset(&self, dataset_name: &str) -> odf::DatasetID {
        let create_dataset_from_snapshot = self
            .catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        let result = create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_name)
                    .kind(odf::DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap();

        result.dataset_handle.id
    }

    async fn delete_dataset(&self, dataset_id: &odf::DatasetID) {
        // Do the actual deletion
        let delete_dataset = self.catalog.get_one::<dyn DeleteDatasetUseCase>().unwrap();
        delete_dataset
            .execute_via_ref(&(dataset_id.as_local_ref()))
            .await
            .unwrap();
    }

    async fn pause_dataset_flow(
        &self,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) {
        self.flow_trigger_service
            .pause_dataset_flows(Utc::now(), &dataset_id, Some(dataset_flow_type))
            .await
            .unwrap();
    }

    async fn pause_all_dataset_flows(&self, dataset_id: odf::DatasetID) {
        self.flow_trigger_service
            .pause_dataset_flows(Utc::now(), &dataset_id, None)
            .await
            .unwrap();
    }

    async fn pause_system_flow(&self, system_flow_type: SystemFlowType) {
        self.flow_trigger_service
            .pause_system_flows(Utc::now(), Some(system_flow_type))
            .await
            .unwrap();
    }

    async fn resume_dataset_flow(
        &self,
        dataset_id: odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) {
        self.flow_trigger_service
            .resume_dataset_flows(Utc::now(), &dataset_id, Some(dataset_flow_type))
            .await
            .unwrap();
    }

    async fn resume_all_dataset_flows(&self, dataset_id: odf::DatasetID) {
        self.flow_trigger_service
            .resume_dataset_flows(Utc::now(), &dataset_id, None)
            .await
            .unwrap();
    }

    async fn resume_system_flow(&self, system_flow_type: SystemFlowType) {
        self.flow_trigger_service
            .resume_system_flows(Utc::now(), Some(system_flow_type))
            .await
            .unwrap();
    }

    fn trigger_events_count(&self) -> usize {
        self.trigger_listener.triggers_events_count()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
