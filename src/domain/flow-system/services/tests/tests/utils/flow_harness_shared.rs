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
use kamu::testing::{MetadataFactory, MockDatasetChangesService};
use kamu::*;
use kamu_accounts::{
    AccountConfig,
    AuthenticationService,
    CurrentAccountSubject,
    JwtAuthenticationConfig,
    PredefinedAccountsConfig,
};
use kamu_accounts_inmem::{InMemoryAccessTokenRepository, InMemoryAccountRepository};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AuthenticationServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_core::*;
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::*;
use kamu_task_system::{TaskProgressMessage, MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR};
use kamu_task_system_inmem::InMemoryTaskSystemEventStore;
use kamu_task_system_services::TaskSchedulerImpl;
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxImmediateImpl};
use opendatafabric::*;
use time_source::{FakeSystemTimeSource, SystemTimeSource};
use tokio::task::yield_now;

use super::{
    FlowSystemTestListener,
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
    _tmp_dir: tempfile::TempDir,
    pub catalog: dill::Catalog,
    pub dataset_repo: Arc<dyn DatasetRepository>,
    pub flow_configuration_service: Arc<dyn FlowConfigurationService>,
    pub flow_service: Arc<dyn FlowService>,
    pub auth_svc: Arc<dyn AuthenticationService>,
    pub fake_system_time_source: FakeSystemTimeSource,
}

#[derive(Default)]
pub(crate) struct FlowHarnessOverrides {
    pub awaiting_step: Option<Duration>,
    pub mandatory_throttling_period: Option<Duration>,
    pub mock_dataset_changes: Option<MockDatasetChangesService>,
    pub custom_account_names: Vec<AccountName>,
    pub is_multi_tenant: bool,
}

impl FlowHarness {
    pub async fn new() -> Self {
        Self::with_overrides(FlowHarnessOverrides::default()).await
    }

    pub async fn with_overrides(overrides: FlowHarnessOverrides) -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = tmp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let t = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();
        let fake_system_time_source = FakeSystemTimeSource::new(t);

        let awaiting_step = overrides
            .awaiting_step
            .unwrap_or(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap());

        let mandatory_throttling_period = overrides.mandatory_throttling_period.unwrap_or(
            Duration::try_milliseconds(SCHEDULING_MANDATORY_THROTTLING_PERIOD_MS).unwrap(),
        );

        let mock_dataset_changes = overrides.mock_dataset_changes.unwrap_or_default();

        let predefined_accounts_config = if overrides.custom_account_names.is_empty() {
            PredefinedAccountsConfig::single_tenant()
        } else {
            let mut predefined_accounts_config = PredefinedAccountsConfig::new();
            for account_name in overrides.custom_account_names {
                predefined_accounts_config
                    .predefined
                    .push(AccountConfig::from_name(account_name));
            }
            predefined_accounts_config
        };

        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<FlowSystemTestListener>()
            .add_value(FlowServiceRunConfig::new(
                awaiting_step,
                mandatory_throttling_period,
            ))
            .add::<FlowServiceImpl>()
            .add::<InMemoryFlowEventStore>()
            .add::<FlowConfigurationServiceImpl>()
            .add::<InMemoryFlowConfigurationEventStore>()
            .add_value(fake_system_time_source.clone())
            .bind::<dyn SystemTimeSource, FakeSystemTimeSource>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(overrides.is_multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(mock_dataset_changes)
            .bind::<dyn DatasetChangesService, MockDatasetChangesService>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<DeleteDatasetUseCaseImpl>()
            .add::<AuthenticationServiceImpl>()
            .add_value(predefined_accounts_config)
            .add_value(JwtAuthenticationConfig::default())
            .add::<InMemoryAccountRepository>()
            .add::<AccessTokenServiceImpl>()
            .add::<InMemoryAccessTokenRepository>()
            .add::<DependencyGraphServiceInMemory>()
            .add::<DatasetOwnershipServiceInMemory>()
            .add::<DatasetOwnershipServiceInMemoryStateInitializer>()
            .add::<TaskSchedulerImpl>()
            .add::<InMemoryTaskSystemEventStore>()
            .add::<LoginPasswordAuthProvider>()
            .add::<PredefinedAccountsRegistrator>()
            .add::<DatabaseTransactionRunner>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
            );
            register_message_dispatcher::<TaskProgressMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
            );
            register_message_dispatcher::<FlowConfigurationUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
            );
            register_message_dispatcher::<FlowServiceUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_SERVICE,
            );

            b.build()
        };

        DatabaseTransactionRunner::new(catalog.clone())
            .transactional(|transactional_catalog| async move {
                let registrator = transactional_catalog
                    .get_one::<PredefinedAccountsRegistrator>()
                    .unwrap();

                registrator
                    .ensure_predefined_accounts_are_registered()
                    .await
            })
            .await
            .unwrap();

        let flow_service = catalog.get_one::<dyn FlowService>().unwrap();
        let flow_configuration_service = catalog.get_one::<dyn FlowConfigurationService>().unwrap();
        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let auth_svc = catalog.get_one::<dyn AuthenticationService>().unwrap();

        Self {
            _tmp_dir: tmp_dir,
            catalog,
            flow_service,
            flow_configuration_service,
            dataset_repo,
            fake_system_time_source,
            auth_svc,
        }
    }

    pub async fn create_root_dataset(&self, dataset_alias: DatasetAlias) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_alias)
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap()
    }

    pub async fn create_derived_dataset(
        &self,
        dataset_alias: DatasetAlias,
        input_ids: Vec<DatasetID>,
    ) -> DatasetID {
        let create_dataset_from_snapshot = self
            .catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        let create_result = create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_alias)
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(input_ids)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();

        create_result.dataset_handle.id
    }

    pub async fn eager_initialization(&self) {
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

        self.catalog
            .get_one::<DatasetOwnershipServiceInMemoryStateInitializer>()
            .unwrap()
            .eager_initialization()
            .await
            .unwrap();
    }

    pub async fn delete_dataset(&self, dataset_id: &DatasetID) {
        // Eagerly push dependency graph initialization before deletes.
        // It's ignored, if requested 2nd time
        self.eager_initialization().await;

        // Do the actual deletion
        let delete_dataset = self.catalog.get_one::<dyn DeleteDatasetUseCase>().unwrap();
        delete_dataset
            .execute_via_ref(&(dataset_id.as_local_ref()))
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_schedule(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        schedule: Schedule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                request_time,
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                FlowConfigurationRule::Schedule(schedule),
            )
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_reset_rule(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        reset_rule: ResetRule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                request_time,
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                FlowConfigurationRule::ResetRule(reset_rule),
            )
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_batching_rule(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        batching_rule: BatchingRule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                request_time,
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                FlowConfigurationRule::BatchingRule(batching_rule),
            )
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_compaction_rule(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        compaction_rule: CompactionRule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                request_time,
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                FlowConfigurationRule::CompactionRule(compaction_rule),
            )
            .await
            .unwrap();
    }

    pub async fn pause_dataset_flow(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let current_config = self
            .flow_configuration_service
            .find_configuration(flow_key.clone())
            .await
            .unwrap()
            .unwrap();

        self.flow_configuration_service
            .set_configuration(request_time, flow_key, true, current_config.rule)
            .await
            .unwrap();
    }

    pub async fn resume_dataset_flow(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let current_config = self
            .flow_configuration_service
            .find_configuration(flow_key.clone())
            .await
            .unwrap()
            .unwrap();

        self.flow_configuration_service
            .set_configuration(request_time, flow_key, false, current_config.rule)
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
        ManualFlowTriggerDriver::new(
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            args,
        )
    }

    pub fn now_datetime(&self) -> DateTime<Utc> {
        self.fake_system_time_source.now()
    }

    pub async fn advance_time(&self, time_quantum: Duration) {
        self.advance_time_custom_alignment(
            Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap(),
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
