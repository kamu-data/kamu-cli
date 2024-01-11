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
use event_bus::EventBus;
use futures::TryStreamExt;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::*;
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use opendatafabric::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_visibility() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());

    let gc_schedule: Schedule = Duration::minutes(30).into();
    harness
        .set_system_flow_schedule(SystemFlowType::GC, gc_schedule.clone())
        .await;

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;

    let foo_ingest_schedule: Schedule = Duration::days(1).into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_schedule.clone(),
        )
        .await;

    let foo_compaction_schedule: Schedule = Duration::weeks(1).into();
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Compaction,
            foo_compaction_schedule.clone(),
        )
        .await;

    let bar_ingest_schedule: Schedule = Duration::hours(3).into();
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
            DatasetFlowType::Compaction,
            &foo_compaction_schedule,
        ),
        (bar_id, DatasetFlowType::Ingest, &bar_ingest_schedule),
    ] {
        harness.expect_dataset_flow_schedule(&configs, dataset_id, dataset_flow_type, schedule);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());

    // Make a dataset and configure daily ingestion schedule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_schedule: Schedule = Duration::days(1).into();
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

    // Now, pause this flow configuration
    harness
        .pause_dataset_flow(foo_id.clone(), DatasetFlowType::Ingest)
        .await;

    // It should disappear from the list of enabled configs
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(0, configs.len());

    // Still we should see it's state as paused in the repository directly
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

    // Now, resume the configuraton
    harness
        .resume_dataset_flow(foo_id.clone(), DatasetFlowType::Ingest)
        .await;

    // It should be visible in the list of active configs again
    let configs = harness.list_enabled_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_schedule(
        &configs,
        foo_id,
        DatasetFlowType::Ingest,
        &foo_ingest_schedule,
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());

    // Make a dataset and configure daily ingestion schedule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_schedule: Schedule = Duration::days(1).into();
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

    // Now make the schedule weekly
    let foo_ingest_schedule_2: Schedule = Duration::weeks(1).into();
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
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_enabled_configurations().await.is_empty());

    // Make a dataset and configure daily ingestion schedule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_schedule: Schedule = Duration::days(1).into();
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

    // Still we should see it's state as permanently stopped in the repository
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

/////////////////////////////////////////////////////////////////////////////////////////

struct FlowConfigurationHarness {
    _tmp_dir: tempfile::TempDir,
    catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    flow_configuration_event_store: Arc<dyn FlowConfigurationEventStore>,
}

impl FlowConfigurationHarness {
    fn new() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = tmp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        use dill::Component;
        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add::<FlowConfigurationServiceInMemory>()
            .add::<FlowConfigurationEventStoreInMem>()
            .add::<SystemTimeSourceDefault>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceInMemory>()
            .build();

        let flow_configuration_service = catalog.get_one::<dyn FlowConfigurationService>().unwrap();
        let flow_configuration_event_store = catalog
            .get_one::<dyn FlowConfigurationEventStore>()
            .unwrap();
        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        Self {
            _tmp_dir: tmp_dir,
            catalog,
            flow_configuration_service,
            flow_configuration_event_store,
            dataset_repo,
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

    async fn set_system_flow_schedule(&self, system_flow_type: SystemFlowType, schedule: Schedule) {
        self.flow_configuration_service
            .set_configuration(
                Utc::now(),
                system_flow_type.into(),
                false,
                FlowConfigurationRule::Schedule(schedule),
            )
            .await
            .unwrap();
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
                flow_key: _,
                status: FlowConfigurationStatus::Active,
                rule: FlowConfigurationRule::Schedule(actual_schedule)
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
                flow_key: _,
                status: FlowConfigurationStatus::Active,
                rule: FlowConfigurationRule::Schedule(actual_schedule)
            }) if actual_schedule == expected_schedule
        );
    }

    async fn pause_dataset_flow(&self, dataset_id: DatasetID, dataset_flow_type: DatasetFlowType) {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let current_config = self
            .flow_configuration_service
            .find_configuration(flow_key.clone())
            .await
            .unwrap()
            .unwrap();

        self.flow_configuration_service
            .set_configuration(Utc::now(), flow_key, true, current_config.rule)
            .await
            .unwrap();
    }

    async fn resume_dataset_flow(&self, dataset_id: DatasetID, dataset_flow_type: DatasetFlowType) {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let current_config = self
            .flow_configuration_service
            .find_configuration(flow_key.clone())
            .await
            .unwrap()
            .unwrap();

        self.flow_configuration_service
            .set_configuration(Utc::now(), flow_key, false, current_config.rule)
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

    async fn create_root_dataset(&self, dataset_name: &str) -> DatasetID {
        let result = self
            .dataset_repo
            .create_dataset_from_snapshot(
                None,
                MetadataFactory::dataset_snapshot()
                    .name(DatasetName::new_unchecked(dataset_name))
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
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
        self.dataset_repo
            .delete_dataset(&(dataset_id.as_local_ref()))
            .await
            .unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
