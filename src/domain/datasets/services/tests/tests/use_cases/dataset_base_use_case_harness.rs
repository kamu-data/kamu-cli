// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::sync::Arc;

use dill::{Catalog, CatalogBuilder, InjectionError};
use kamu::testing::{BaseRepoHarness, MockDatasetActionAuthorizer};
use kamu_accounts::*;
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
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
use odf::dataset::MetadataChainExt;
use time_source::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetBaseUseCaseHarness {
    _temp_dir: tempfile::TempDir,
    no_subject_catalog: Catalog,
    intermediate_catalog: Catalog,
    _did_generator: Arc<dyn DidGenerator>,
    system_time_source: Arc<dyn SystemTimeSource>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    test_dataset_outbox_listener: Arc<TestDatasetOutboxListener>,
}

impl DatasetBaseUseCaseHarness {
    pub async fn new(opts: DatasetBaseUseCaseHarnessOpts<'_>) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();

        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let run_info_dir = temp_dir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let no_subject_catalog = {
            let mut b = if let Some(base_catalog) = opts.maybe_base_catalog {
                CatalogBuilder::new_chained(base_catalog)
            } else {
                CatalogBuilder::new()
            };

            b.add_value(RunInfoDir::new(run_info_dir))
                .add_value(opts.tenancy_config)
                .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                    datasets_dir,
                ))
                .add::<DatasetLfsBuilderDatabaseBackedImpl>()
                .add_value(kamu_datasets_services::MetadataChainDbBackedConfig::default())
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>()
                .add::<InMemoryDidSecretKeyRepository>()
                .add_value(DidSecretEncryptionConfig::sample())
                .add::<DatasetAliasUpdateHandler>()
                .add::<AccountServiceImpl>()
                .add::<InMemoryAccountRepository>()
                .add::<DatasetReferenceServiceImpl>()
                .add::<InMemoryDatasetReferenceRepository>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add::<InMemoryDatasetKeyBlockRepository>()
                .add::<InMemoryDatasetDataBlockRepository>()
                .add::<DatasetBlockUpdateHandler>()
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

            opts.outbox_provider.embed_into_catalog(&mut b);

            opts.system_time_source_provider.embed_into_catalog(&mut b);

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

            register_message_dispatcher::<DatasetKeyBlocksMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_KEY_BLOCK_UPDATE_HANDLER,
            );

            b.build()
        };

        let intermediate_catalog = {
            let mut b = CatalogBuilder::new_chained(&no_subject_catalog);
            b.add_value(CurrentAccountSubject::new_test());
            b.build()
        };

        let account_repo = intermediate_catalog
            .get_one::<dyn AccountRepository>()
            .unwrap();
        account_repo.save_account(&Account::dummy()).await.unwrap();

        Self {
            _temp_dir: temp_dir,
            _did_generator: intermediate_catalog.get_one().unwrap(),
            system_time_source: intermediate_catalog.get_one().unwrap(),
            dataset_registry: intermediate_catalog.get_one().unwrap(),
            test_dataset_outbox_listener: intermediate_catalog.get_one().unwrap(),
            no_subject_catalog,
            intermediate_catalog,
        }
    }

    pub fn intermediate_catalog(&self) -> &Catalog {
        &self.intermediate_catalog
    }

    pub fn no_subject_catalog(&self) -> &Catalog {
        &self.no_subject_catalog
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

    pub async fn create_root_dataset(
        &self,
        catalog: &dill::Catalog,
        alias: &odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let use_case = match catalog.get_one::<dyn CreateDatasetFromSnapshotUseCase>() {
            Ok(use_case) => use_case,
            Err(InjectionError::Unregistered(_)) => {
                let mut b = CatalogBuilder::new_chained(catalog);
                b.add::<CreateDatasetFromSnapshotUseCaseImpl>();
                b.add::<CreateDatasetUseCaseHelper>();
                let catalog = b.build();
                catalog
                    .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
                    .unwrap()
            }
            Err(e) => panic!("Failed to get CreateDatasetFromSnapshotUseCase: {e}"),
        };

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
        catalog: &dill::Catalog,
        alias: &odf::DatasetAlias,
        input_dataset_refs: Vec<odf::DatasetRef>,
    ) -> CreateDatasetResult {
        let use_case = match catalog.get_one::<dyn CreateDatasetFromSnapshotUseCase>() {
            Ok(use_case) => use_case,
            Err(InjectionError::Unregistered(_)) => {
                let mut b = CatalogBuilder::new_chained(catalog);
                b.add::<CreateDatasetFromSnapshotUseCaseImpl>();
                b.add::<CreateDatasetUseCaseHelper>();
                let catalog = b.build();
                catalog
                    .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
                    .unwrap()
            }
            Err(e) => panic!("Failed to get CreateDatasetFromSnapshotUseCase: {e}"),
        };

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

    pub async fn append_dataset_metadata(
        &self,
        catalog: &dill::Catalog,
        dataset: ResolvedDataset,
        events: Vec<odf::MetadataEvent>,
    ) -> Option<odf::Multihash> {
        // Resolve or build use case
        let use_case = match catalog.get_one::<dyn AppendDatasetMetadataBatchUseCase>() {
            Ok(use_case) => use_case,
            Err(InjectionError::Unregistered(_)) => {
                let mut b = CatalogBuilder::new_chained(catalog);
                b.add::<AppendDatasetMetadataBatchUseCaseImpl>();
                let catalog = b.build();
                catalog
                    .get_one::<dyn AppendDatasetMetadataBatchUseCase>()
                    .unwrap()
            }
            Err(e) => panic!("Failed to get AppendDatasetMetadataBatchUseCase: {e}"),
        };

        // Resove current head
        let head = dataset
            .as_metadata_chain()
            .try_get_ref(&odf::BlockRef::Head)
            .await
            .unwrap()
            .unwrap();
        let head_block = dataset.as_metadata_chain().get_block(&head).await.unwrap();

        // Build new blocks with hashes
        let mut prev_block_hash = head;
        let mut prev_block_sequence_number = head_block.sequence_number;

        let mut new_hashed_blocks = VecDeque::with_capacity(events.len());
        for event in events {
            let block = odf::MetadataBlock {
                prev_block_hash: Some(prev_block_hash),
                sequence_number: prev_block_sequence_number + 1,
                event,
                system_time: self.system_time_source.now(),
            };
            let hash = BaseRepoHarness::hash_from_block(&block);
            new_hashed_blocks.push_back((hash.clone(), block));

            prev_block_hash = hash;
            prev_block_sequence_number += 1;
        }

        // Execute append use case
        use_case
            .execute(
                dataset.as_ref(),
                Box::new(new_hashed_blocks.into_iter()),
                AppendDatasetMetadataBatchUseCaseOptions::default(),
            )
            .await
            .unwrap()
    }

    pub async fn rename_dataset(
        &self,
        catalog: &dill::Catalog,
        dataset_id: &odf::DatasetID,
        new_name: &str,
    ) {
        let use_case = match catalog.get_one::<dyn RenameDatasetUseCase>() {
            Ok(use_case) => use_case,
            Err(InjectionError::Unregistered(_)) => {
                let mut b = CatalogBuilder::new_chained(catalog);
                b.add::<RenameDatasetUseCaseImpl>();
                let catalog = b.build();
                catalog.get_one::<dyn RenameDatasetUseCase>().unwrap()
            }
            Err(e) => panic!("Failed to get RenameDatasetUseCase: {e}"),
        };

        use_case
            .execute(
                &dataset_id.as_local_ref(),
                &odf::DatasetName::new_unchecked(new_name),
            )
            .await
            .unwrap();
    }

    pub async fn delete_dataset(&self, catalog: &dill::Catalog, dataset_id: &odf::DatasetID) {
        let use_case = match catalog.get_one::<dyn DeleteDatasetUseCase>() {
            Ok(use_case) => use_case,
            Err(InjectionError::Unregistered(_)) => {
                let mut b = CatalogBuilder::new_chained(catalog);
                b.add::<DeleteDatasetUseCaseImpl>();
                let catalog = b.build();
                catalog.get_one::<dyn DeleteDatasetUseCase>().unwrap()
            }
            Err(e) => panic!("Failed to get DeleteDatasetUseCase: {e}"),
        };

        use_case
            .execute_via_ref(&dataset_id.as_local_ref())
            .await
            .unwrap();
    }

    pub fn collected_outbox_messages(&self) -> String {
        format!("{}", self.test_dataset_outbox_listener.as_ref())
    }

    pub fn reset_collected_outbox_messages(&self) {
        self.test_dataset_outbox_listener.reset();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetBaseUseCaseHarnessOpts<'a> {
    pub maybe_base_catalog: Option<&'a dill::Catalog>,
    pub tenancy_config: TenancyConfig,
    pub maybe_mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    pub maybe_mock_did_generator: Option<MockDidGenerator>,
    pub outbox_provider: OutboxProvider,
    pub system_time_source_provider: SystemTimeSourceProvider,
}

impl Default for DatasetBaseUseCaseHarnessOpts<'_> {
    fn default() -> Self {
        Self {
            maybe_base_catalog: None,
            tenancy_config: TenancyConfig::SingleTenant,
            maybe_mock_dataset_action_authorizer: None,
            maybe_mock_did_generator: None,
            // Use immediate outbox in all dataset use case harnesses
            outbox_provider: OutboxProvider::Immediate {
                force_immediate: true,
            },
            system_time_source_provider: SystemTimeSourceProvider::Default,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
