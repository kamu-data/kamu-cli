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

use dill::*;
use futures::TryStreamExt;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::DependencyGraphServiceImpl;
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
    assert!(harness.list_active_configurations().await.is_empty());

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;

    let foo_ingest_config = FlowConfigurationRule::IngestRule(IngestRule {
        fetch_uncacheable: false,
    });
    harness
        .set_dataset_flow_config(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_config.clone(),
        )
        .await;

    let foo_compaction_config = FlowConfigurationRule::CompactionRule(CompactionRule::Full(
        CompactionRuleFull::new_checked(2, 3, false).unwrap(),
    ));
    harness
        .set_dataset_flow_config(
            foo_id.clone(),
            DatasetFlowType::HardCompaction,
            foo_compaction_config.clone(),
        )
        .await;

    let bar_compaction_config = FlowConfigurationRule::CompactionRule(CompactionRule::Full(
        CompactionRuleFull::new_checked(3, 4, false).unwrap(),
    ));
    harness
        .set_dataset_flow_config(
            bar_id.clone(),
            DatasetFlowType::HardCompaction,
            bar_compaction_config.clone(),
        )
        .await;

    let configs = harness.list_active_configurations().await;
    assert_eq!(3, configs.len());

    for (dataset_id, dataset_flow_type, config) in [
        (foo_id.clone(), DatasetFlowType::Ingest, &foo_ingest_config),
        (
            foo_id,
            DatasetFlowType::HardCompaction,
            &foo_compaction_config,
        ),
        (
            bar_id,
            DatasetFlowType::HardCompaction,
            &bar_compaction_config,
        ),
    ] {
        harness.expect_dataset_flow_config(&configs, dataset_id, dataset_flow_type, config);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_active_configurations().await.is_empty());
    assert_eq!(0, harness.configuration_events_count());

    // Make a dataset and configure compaction config
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_compaction_config = FlowConfigurationRule::CompactionRule(CompactionRule::Full(
        CompactionRuleFull::new_checked(1, 2, false).unwrap(),
    ));
    harness
        .set_dataset_flow_config(
            foo_id.clone(),
            DatasetFlowType::HardCompaction,
            foo_compaction_config.clone(),
        )
        .await;

    // It should be visible in the list of configs
    let configs = harness.list_active_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_config(
        &configs,
        foo_id.clone(),
        DatasetFlowType::HardCompaction,
        &foo_compaction_config,
    );

    // Now make the config with different parameters
    let foo_compaction_config_2 = FlowConfigurationRule::CompactionRule(CompactionRule::Full(
        CompactionRuleFull::new_checked(2, 3, false).unwrap(),
    ));
    harness
        .set_dataset_flow_config(
            foo_id.clone(),
            DatasetFlowType::HardCompaction,
            foo_compaction_config_2.clone(),
        )
        .await;

    // Observe the updated config
    let configs = harness.list_active_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_config(
        &configs,
        foo_id.clone(),
        DatasetFlowType::HardCompaction,
        &foo_compaction_config_2,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_active_configurations().await.is_empty());

    // Make a dataset and configure ingest rule
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_ingest_config = FlowConfigurationRule::IngestRule(IngestRule {
        fetch_uncacheable: true,
    });
    harness
        .set_dataset_flow_config(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            foo_ingest_config.clone(),
        )
        .await;

    // It should be visible in the list of configs
    let configs = harness.list_active_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_config(
        &configs,
        foo_id.clone(),
        DatasetFlowType::Ingest,
        &foo_ingest_config,
    );

    // Now, delete the dataset
    harness.delete_dataset(&foo_id).await;

    // The dataset should not be visible in the list of configs
    let configs = harness.list_active_configurations().await;
    assert_eq!(0, configs.len());

    // Still, we should see it's state as permanently stopped in the repository
    let flow_config_state = harness
        .get_dataset_flow_config_from_store(foo_id, DatasetFlowType::Ingest)
        .await;
    assert_eq!(
        flow_config_state.rule,
        FlowConfigurationRule::IngestRule(IngestRule {
            fetch_uncacheable: true,
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct FlowConfigurationHarness {
    _tmp_dir: tempfile::TempDir,
    catalog: Catalog,
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
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<DatasetRegistryRepoBridge>()
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
        let flow_config_events_listener = catalog.get_one::<FlowConfigTestListener>().unwrap();

        Self {
            _tmp_dir: tmp_dir,
            catalog,
            flow_configuration_service,
            flow_configuration_event_store,
            config_listener: flow_config_events_listener,
        }
    }

    async fn list_active_configurations(&self) -> HashMap<FlowKey, FlowConfigurationState> {
        let active_configs: Vec<_> = self
            .flow_configuration_service
            .list_active_configurations()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut res = HashMap::new();
        for active_config in active_configs {
            res.insert(active_config.flow_key.clone(), active_config);
        }
        res
    }

    async fn set_dataset_flow_config(
        &self,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        configuration_rule: FlowConfigurationRule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                configuration_rule,
            )
            .await
            .unwrap();
    }

    fn expect_dataset_flow_config(
        &self,
        configurations: &HashMap<FlowKey, FlowConfigurationState>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        expected_rule: &FlowConfigurationRule,
    ) {
        assert_matches!(
            configurations.get(&(FlowKeyDataset::new(dataset_id, dataset_flow_type).into())),
            Some(FlowConfigurationState {
                status: FlowConfigurationStatus::Active,
                rule: actual_rule,
                ..
            }) if actual_rule == expected_rule
        );
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
