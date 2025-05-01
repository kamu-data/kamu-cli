// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crypto_utils::DidSecretEncryptionConfig;
use dill::{Catalog, CatalogBuilder, Component};
use kamu::testing::MockDatasetActionAuthorizer;
use kamu_accounts::*;
use kamu_accounts_inmem::{InMemoryAccountDidSecretKeyRepository, InMemoryAccountRepository};
use kamu_accounts_services::AccountServiceImpl;
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_core::auth::{AlwaysHappyDatasetActionAuthorizer, DatasetActionAuthorizer};
use kamu_core::*;
use kamu_datasets::*;
use kamu_datasets_inmem::*;
use kamu_datasets_services::testing::TestDatasetOutboxListener;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::*;
use time_source::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetBaseUseCaseHarness {
    _temp_dir: tempfile::TempDir,
    catalog: Catalog,
    _did_generator: Arc<dyn DidGenerator>,
    system_time_source: Arc<dyn SystemTimeSource>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    test_dataset_outbox_listener: Arc<TestDatasetOutboxListener>,
}

impl DatasetBaseUseCaseHarness {
    pub async fn new(opts: DatasetBaseUseCaseHarnessOpts) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();

        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let run_info_dir = temp_dir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add_value(RunInfoDir::new(run_info_dir))
                .add_value(opts.tenancy_config)
                .add_value(CurrentAccountSubject::new_test())
                .add_builder(
                    OutboxImmediateImpl::builder()
                        .with_consumer_filter(ConsumerFilter::AllConsumers),
                )
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                    datasets_dir,
                ))
                .add::<DatasetLfsBuilderDatabaseBackedImpl>()
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>()
                .add::<InMemoryDatasetDidSecretKeyRepository>()
                .add::<InMemoryAccountDidSecretKeyRepository>()
                .add_value(DidSecretEncryptionConfig::sample())
                .add::<DatasetAliasUpdateHandler>()
                .add::<AccountServiceImpl>()
                .add::<InMemoryAccountRepository>()
                .add::<DatasetReferenceServiceImpl>()
                .add::<InMemoryDatasetReferenceRepository>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add::<InMemoryDatasetKeyBlockRepository>()
                .add::<DatasetKeyBlockUpdateHandler>()
                .add::<DependencyGraphImmediateListener>()
                .add::<RebacDatasetRegistryFacadeImpl>()
                .add::<TestDatasetOutboxListener>();

            if let Some(mock_dataset_action_authorizer) = opts.maybe_mock_dataset_action_authorizer
            {
                b.add_value(mock_dataset_action_authorizer)
                    .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>();
            } else {
                b.add::<AlwaysHappyDatasetActionAuthorizer>();
            }

            if let Some(mock_did_generator) = opts.maybe_mock_did_generator {
                b.add_value(mock_did_generator)
                    .bind::<dyn DidGenerator, MockDidGenerator>();
            } else {
                b.add::<DidGeneratorDefault>();
            }

            if let Some(system_time_source_stub) = opts.maybe_system_time_source_stub {
                b.add_value(system_time_source_stub)
                    .bind::<dyn SystemTimeSource, SystemTimeSourceStub>();
            } else {
                b.add::<SystemTimeSourceDefault>();
            }

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );

            register_message_dispatcher::<DatasetReferenceMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
            );

            register_message_dispatcher::<DatasetDependenciesMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
            );

            b.build()
        };

        let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();
        account_repo.save_account(&Account::dummy()).await.unwrap();

        Self {
            _temp_dir: temp_dir,
            _did_generator: catalog.get_one().unwrap(),
            system_time_source: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            test_dataset_outbox_listener: catalog.get_one().unwrap(),
            catalog,
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn system_time_source(&self) -> &dyn SystemTimeSource {
        self.system_time_source.as_ref()
    }

    pub async fn check_dataset_exists(
        &self,
        alias: &odf::DatasetAlias,
    ) -> Result<(), odf::DatasetRefUnresolvedError> {
        self.dataset_registry
            .get_dataset_by_ref(&alias.as_local_ref())
            .await?;
        Ok(())
    }

    pub async fn create_root_dataset(&self, alias: &odf::DatasetAlias) -> CreateDatasetResult {
        let mut b = CatalogBuilder::new_chained(&self.catalog);
        b.add::<CreateDatasetFromSnapshotUseCaseImpl>();
        b.add::<CreateDatasetUseCaseHelper>();

        let catalog = b.build();
        let use_case = catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        use odf::metadata::testing::MetadataFactory;
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build();

        use_case
            .execute(snapshot, CreateDatasetUseCaseOptions::default())
            .await
            .unwrap()
    }

    pub async fn create_derived_dataset(
        &self,
        alias: &odf::DatasetAlias,
        input_dataset_refs: Vec<odf::DatasetRef>,
    ) -> CreateDatasetResult {
        let mut b = CatalogBuilder::new_chained(&self.catalog);
        b.add::<CreateDatasetFromSnapshotUseCaseImpl>();
        b.add::<CreateDatasetUseCaseHelper>();

        let catalog = b.build();
        let use_case = catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        use odf::metadata::testing::MetadataFactory;
        let snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Derivative)
            .push_event(
                MetadataFactory::set_transform()
                    .inputs_from_refs(input_dataset_refs)
                    .build(),
            )
            .build();

        use_case
            .execute(snapshot, CreateDatasetUseCaseOptions::default())
            .await
            .unwrap()
    }

    pub fn collected_outbox_messages(&self) -> String {
        format!("{}", self.test_dataset_outbox_listener.as_ref())
    }

    pub fn reset_collected_outbox_messages(&self) {
        self.test_dataset_outbox_listener.reset();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct DatasetBaseUseCaseHarnessOpts {
    pub tenancy_config: TenancyConfig,
    pub maybe_mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    pub maybe_mock_did_generator: Option<MockDidGenerator>,
    pub maybe_system_time_source_stub: Option<SystemTimeSourceStub>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
