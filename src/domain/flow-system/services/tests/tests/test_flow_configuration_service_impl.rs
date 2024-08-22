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
use database_common_macros::transactional_method;
use dill::*;
use futures::TryStreamExt;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::RebacServiceImpl;
use kamu_core::*;
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::*;
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxImmediateImpl};
use opendatafabric::*;
use time_source::SystemTimeSourceDefault;

use super::FlowConfigTestListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_visibility() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());

    let gc_schedule: Schedule = Duration::try_minutes(30).unwrap().into();
    harness
        .set_system_flow_schedule(SystemFlowType::GC, gc_schedule.clone())
        .await;

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;

    let foo_ingest_schedule: Schedule = Duration::try_days(1).unwrap().into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_schedule.clone(),
        )
        .await;

    let foo_compaction_schedule: Schedule = Duration::try_weeks(1).unwrap().into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::HardCompaction,
            foo_compaction_schedule.clone(),
        )
        .await;

    let bar_ingest_schedule: Schedule = Duration::try_hours(3).unwrap().into();
    harness
        .set_dataset_flow_schedule(
            bar_id.clone(),
            DatasetFlowType::Ingest,
            bar_ingest_schedule.clone(),
        )
        .await;

    let configs = harness.list_enabled_configurations().await;
    assert_eq!(4, configs.len());

    harness.expect_system_flow_schedule(&configs, SystemFlowType::GC, &gc_schedule);

    for (dataset_id, dataset_flow_type, schedule) in [
        (
            foo_id.clone(),
            DatasetFlowType::Ingest,
            &foo_ingest_schedule,
        ),
        (
            foo_id,
            DatasetFlowType::HardCompaction,
            &foo_compaction_schedule,
        ),
        (bar_id, DatasetFlowType::Ingest, &bar_ingest_schedule),
    ] {
        harness.expect_dataset_flow_schedule(&configs, dataset_id, dataset_flow_type, schedule);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_individual_dataset_flows() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());
    assert_eq!(0, harness.configuration_events_count());

    // Make a dataset and configure daily ingestion schedule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_schedule: Schedule = Duration::try_days(1).unwrap().into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_schedule.clone(),
        )
        .await;

    // It should be visible in the list of enabled configs and produced 1 config
    // event
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_schedule,
    );
    assert_eq!(1, harness.configuration_events_count());

    // Now, pause this flow configuration
    harness
        .pause_dataset_flow(foo_id.clone(), DatasetFlowType::Ingest)
        .await;

    // It should disappear from the list of enabled configs, and produce 1 more
    // event
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(0, configs.len());
    assert_eq!(2, harness.configuration_events_count());

    // Still, we should see it's state as paused in the repository directly
    let flow_config_state = harness
        .get_dataset_flow_config_from_store(foo_id.clone(), DatasetFlowType::Ingest)
        .await;
    assert_eq!(
        flow_config_state.status,
        FlowConfigurationStatus::PausedTemporarily
    );
    assert_eq!(
        flow_config_state.rule,
        FlowConfigurationRule::Schedule(foo_ingest_schedule.clone())
    );

    // Now, resume the configuration
    harness
        .resume_dataset_flow(foo_id.clone(), DatasetFlowType::Ingest)
        .await;

    // It should be visible in the list of active configs again, and produce
    // another event yet
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id,
        DatasetFlowType::Ingest,
        &foo_ingest_schedule,
    );
    assert_eq!(3, harness.configuration_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_all_dataset_flows() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());
    assert_eq!(0, harness.configuration_events_count());

    // Make a dataset and configure ingestion and compaction schedule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_schedule: Schedule = Duration::try_days(1).unwrap().into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_schedule.clone(),
        )
        .await;
    let foo_compaction_schedule: Schedule = Duration::try_weeks(1).unwrap().into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::HardCompaction,
            foo_compaction_schedule.clone(),
        )
        .await;

    // Both should be visible in the list of enabled configs, 2 events expected
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(2, configs.len());
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_schedule,
    );
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id.clone(),
        DatasetFlowType::HardCompaction,
        &foo_compaction_schedule,
    );
    assert_eq!(2, harness.configuration_events_count());

    // Now, pause all flows of this dataset
    harness.pause_all_dataset_flows(foo_id.clone()).await;

    // Both should disappear from the list of enabled configs,
    // and both should produce events
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(0, configs.len());
    assert_eq!(4, harness.configuration_events_count());

    // Still, we should see their state as paused in the repository directly

    let flow_config_ingest_state = harness
        .get_dataset_flow_config_from_store(foo_id.clone(), DatasetFlowType::Ingest)
        .await;
    assert_eq!(
        flow_config_ingest_state.status,
        FlowConfigurationStatus::PausedTemporarily
    );
    assert_eq!(
        flow_config_ingest_state.rule,
        FlowConfigurationRule::Schedule(foo_ingest_schedule.clone())
    );

    let flow_config_compaction_state = harness
        .get_dataset_flow_config_from_store(foo_id.clone(), DatasetFlowType::HardCompaction)
        .await;
    assert_eq!(
        flow_config_compaction_state.status,
        FlowConfigurationStatus::PausedTemporarily
    );
    assert_eq!(
        flow_config_compaction_state.rule,
        FlowConfigurationRule::Schedule(foo_compaction_schedule.clone())
    );

    // Now, resume all configurations
    harness.resume_all_dataset_flows(foo_id.clone()).await;

    // They should be visible in the list of active configs again, and again, we
    // should get 2 extra events
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(2, configs.len());
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_schedule,
    );
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id.clone(),
        DatasetFlowType::HardCompaction,
        &foo_compaction_schedule,
    );
    assert_eq!(6, harness.configuration_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_individual_system_flows() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());
    assert_eq!(0, harness.configuration_events_count());

    // Configure GC schedule
    let gc_schedule: Schedule = Duration::try_minutes(30).unwrap().into();
    harness
        .set_system_flow_schedule(SystemFlowType::GC, gc_schedule.clone())
        .await;

    // It should be visible in the list of enabled configs, and create 1 event
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_system_flow_schedule(&configs, SystemFlowType::GC, &gc_schedule);
    assert_eq!(1, harness.configuration_events_count());

    // Now, pause this flow configuration
    harness.pause_system_flow(SystemFlowType::GC).await;

    // It should disappear from the list of enabled configs, and create 1 more event
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(0, configs.len());
    assert_eq!(2, harness.configuration_events_count());

    // Still, we should see it's state as paused in the repository directly
    let flow_config_state = harness
        .get_system_flow_config_from_store(SystemFlowType::GC)
        .await;
    assert_eq!(
        flow_config_state.status,
        FlowConfigurationStatus::PausedTemporarily
    );
    assert_eq!(
        flow_config_state.rule,
        FlowConfigurationRule::Schedule(gc_schedule.clone())
    );

    // Now, resume the configuration
    harness.resume_system_flow(SystemFlowType::GC).await;

    // It should be visible in the list of active configs again, and create
    // another event yet
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_system_flow_schedule(&configs, SystemFlowType::GC, &gc_schedule);
    assert_eq!(3, harness.configuration_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());
    assert_eq!(0, harness.configuration_events_count());

    // Make a dataset and configure daily ingestion schedule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_schedule: Schedule = Duration::try_days(1).unwrap().into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_schedule.clone(),
        )
        .await;

    // It should be visible in the list of enabled configs
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_schedule,
    );
    assert_eq!(1, harness.configuration_events_count());

    // Now make the schedule weekly
    let foo_ingest_schedule_2: Schedule = Duration::try_weeks(1).unwrap().into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_schedule_2.clone(),
        )
        .await;

    // Observe the updated config
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_schedule_2,
    );
    assert_eq!(2, harness.configuration_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());

    // Make a dataset and configure daily ingestion schedule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_schedule: Schedule = Duration::try_days(1).unwrap().into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_schedule.clone(),
        )
        .await;

    // It should be visible in the list of enabled configs
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_schedule,
    );

    // Now, delete the dataset
    harness.delete_dataset(&foo_id).await;

    // The dataset should not be visible in the list of enabled configs
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(0, configs.len());

    // Still, we should see it's state as permanently stopped in the repository
    let flow_config_state = harness
        .get_dataset_flow_config_from_store(foo_id, DatasetFlowType::Ingest)
        .await;
    assert_eq!(
        flow_config_state.status,
        FlowConfigurationStatus::StoppedPermanently
    );
    assert_eq!(
        flow_config_state.rule,
        FlowConfigurationRule::Schedule(foo_ingest_schedule)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowConfigurationHarness {
    _tmp_dir: tempfile::TempDir,
    catalog: Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    flow_configuration_event_store: Arc<dyn FlowConfigurationEventStore>,
    config_listener: Arc<FlowConfigTestListener>,
}

impl FlowConfigurationHarness {
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
            .add::<FlowConfigTestListener>()
            .add::<FlowConfigurationServiceImpl>()
            .add::<InMemoryFlowConfigurationEventStore>()
            .add::<SystemTimeSourceDefault>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceInMemory>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<DeleteDatasetUseCaseImpl>()
            .add::<InMemoryRebacRepository>()
            .add::<RebacServiceImpl>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
            );
            register_message_dispatcher::<FlowConfigurationUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
            );

            b.build()
        };

        let flow_configuration_service = catalog.get_one::<dyn FlowConfigurationService>().unwrap();
        let flow_configuration_event_store = catalog
            .get_one::<dyn FlowConfigurationEventStore>()
            .unwrap();
        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let flow_config_events_listener = catalog.get_one::<FlowConfigTestListener>().unwrap();

        Self {
            _tmp_dir: tmp_dir,
            catalog,
            flow_configuration_service,
            flow_configuration_event_store,
            dataset_repo,
            config_listener: flow_config_events_listener,
        }
    }

    async fn list_enabled_configurations(&self) -> HashMap<FlowKey, FlowConfigurationState> {
        let active_configs: Vec<_> = self
            .flow_configuration_service
            .list_enabled_configurations()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut res = HashMap::new();
        for active_config in active_configs {
            res.insert(active_config.flow_key.clone(), active_config);
        }
        res
    }

    #[transactional_method(flow_configuration_service: Arc<dyn FlowConfigurationService>)]
    async fn set_system_flow_schedule(&self, system_flow_type: SystemFlowType, schedule: Schedule) {
        flow_configuration_service
            .set_configuration(
                Utc::now(),
                system_flow_type.into(),
                false,
                FlowConfigurationRule::Schedule(schedule),
            )
            .await
    }

    async fn set_dataset_flow_schedule(
        &self,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        schedule: Schedule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                Utc::now(),
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                FlowConfigurationRule::Schedule(schedule),
            )
            .await
            .unwrap();
    }

    fn expect_system_flow_schedule(
        &self,
        enabled_configurations: &HashMap<FlowKey, FlowConfigurationState>,
        system_flow_type: SystemFlowType,
        expected_schedule: &Schedule,
    ) {
        assert_matches!(
            enabled_configurations.get(&(system_flow_type.into())),
            Some(FlowConfigurationState {
                status: FlowConfigurationStatus::Active,
                rule: FlowConfigurationRule::Schedule(actual_schedule),
                ..
            }) if actual_schedule == expected_schedule
        );
    }

    fn expect_dataset_flow_schedule(
        &self,
        enabled_configurations: &HashMap<FlowKey, FlowConfigurationState>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        expected_schedule: &Schedule,
    ) {
        assert_matches!(
            enabled_configurations.get(&(FlowKeyDataset::new(dataset_id, dataset_flow_type).into())),
            Some(FlowConfigurationState {
                status: FlowConfigurationStatus::Active,
                rule: FlowConfigurationRule::Schedule(actual_schedule),
                ..
            }) if actual_schedule == expected_schedule
        );
    }

    async fn pause_dataset_flow(&self, dataset_id: DatasetID, dataset_flow_type: DatasetFlowType) {
        self.flow_configuration_service
            .pause_dataset_flows(Utc::now(), &dataset_id, Some(dataset_flow_type))
            .await
            .unwrap();
    }

    async fn pause_all_dataset_flows(&self, dataset_id: DatasetID) {
        self.flow_configuration_service
            .pause_dataset_flows(Utc::now(), &dataset_id, None)
            .await
            .unwrap();
    }

    async fn pause_system_flow(&self, system_flow_type: SystemFlowType) {
        self.flow_configuration_service
            .pause_system_flows(Utc::now(), Some(system_flow_type))
            .await
            .unwrap();
    }

    async fn resume_dataset_flow(&self, dataset_id: DatasetID, dataset_flow_type: DatasetFlowType) {
        self.flow_configuration_service
            .resume_dataset_flows(Utc::now(), &dataset_id, Some(dataset_flow_type))
            .await
            .unwrap();
    }

    async fn resume_all_dataset_flows(&self, dataset_id: DatasetID) {
        self.flow_configuration_service
            .resume_dataset_flows(Utc::now(), &dataset_id, None)
            .await
            .unwrap();
    }

    async fn resume_system_flow(&self, system_flow_type: SystemFlowType) {
        self.flow_configuration_service
            .resume_system_flows(Utc::now(), Some(system_flow_type))
            .await
            .unwrap();
    }

    async fn get_dataset_flow_config_from_store(
        &self,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) -> FlowConfigurationState {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let flow_configuration =
            FlowConfiguration::load(flow_key, self.flow_configuration_event_store.as_ref())
                .await
                .unwrap();
        flow_configuration.into()
    }

    async fn get_system_flow_config_from_store(
        &self,
        system_flow_type: SystemFlowType,
    ) -> FlowConfigurationState {
        let flow_key: FlowKey = FlowKey::System(FlowKeySystem::new(system_flow_type));
        let flow_configuration =
            FlowConfiguration::load(flow_key, self.flow_configuration_event_store.as_ref())
                .await
                .unwrap();
        flow_configuration.into()
    }

    async fn create_root_dataset(&self, dataset_name: &str) -> DatasetID {
        let create_dataset_from_snapshot = self
            .catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        let result = create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_name)
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap();

        result.dataset_handle.id
    }

    async fn delete_dataset(&self, dataset_id: &DatasetID) {
        // Eagerly push dependency graph initialization before deletes.
        // It's ignored, if requested 2nd time
        let dependency_graph_service = self
            .catalog
            .get_one::<dyn DependencyGraphService>()
            .unwrap();
        let dependency_graph_repository =
            DependencyGraphRepositoryInMemory::new(self.dataset_repo.clone());
        dependency_graph_service
            .eager_initialization(&dependency_graph_repository)
            .await
            .unwrap();

        // Do the actual deletion
        let delete_dataset = self.catalog.get_one::<dyn DeleteDatasetUseCase>().unwrap();
        delete_dataset
            .execute_via_ref(&(dataset_id.as_local_ref()))
            .await
            .unwrap();
    }

    fn configuration_events_count(&self) -> usize {
        self.config_listener.configuration_events_count()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
