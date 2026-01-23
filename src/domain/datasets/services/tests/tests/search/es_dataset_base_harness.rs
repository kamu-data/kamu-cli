// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bon::bon;
use chrono::{DateTime, Utc};
use database_common::NoOpDatabasePlugin;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::InternalError;
use kamu_accounts::*;
use kamu_accounts_services::*;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::*;
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use kamu_messaging_outbox_inmem::{
    InMemoryOutboxMessageConsumptionRepository,
    InMemoryOutboxMessageRepository,
};
use kamu_search::*;
use kamu_search_elasticsearch::testing::{ElasticsearchBaseHarness, ElasticsearchTestContext};
use kamu_search_services::SearchIndexerImpl;
use messaging_outbox::*;
use odf::metadata::testing::MetadataFactory;
use time_source::{SystemTimeSource, SystemTimeSourceProvider, SystemTimeSourceStub};

use crate::tests::use_cases::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
pub struct ElasticsearchDatasetBaseHarness {
    es_base_harness: ElasticsearchBaseHarness,
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    outbox_agent: Arc<dyn OutboxAgent>,
    fixed_time: DateTime<Utc>,
}

#[bon]
impl ElasticsearchDatasetBaseHarness {
    #[builder]
    pub async fn new(
        ctx: Arc<ElasticsearchTestContext>,
        tenancy_config: TenancyConfig,
        predefined_accounts_config: Option<PredefinedAccountsConfig>,
        predefined_datasets_config: Option<PredefinedDatasetsConfig>,
    ) -> Self {
        let fixed_time = Utc::now();

        let es_base_harness = ElasticsearchBaseHarness::new(
            ctx,
            SystemTimeSourceProvider::Stub(SystemTimeSourceStub::new_set(fixed_time)),
        );

        let indexing_catalog = {
            let mut b = dill::CatalogBuilder::new_chained(es_base_harness.catalog());
            // Outbox repositories
            b.add::<InMemoryOutboxMessageRepository>();
            b.add::<InMemoryOutboxMessageConsumptionRepository>();

            // Search
            b.add::<DatasetSearchSchemaProvider>();
            b.add::<DatasetSearchUpdater>();

            // Supplementary use cases
            b.add::<DatasetAccountLifecycleHandler>();
            b.add::<DeleteDatasetUseCaseImpl>();

            // Embedding mocks
            let mut embeddings_chunker = MockEmbeddingsChunker::new();
            embeddings_chunker.expect_chunk().returning(Ok);

            let mut embeddings_encoder = MockEmbeddingsEncoder::new();
            embeddings_encoder.expect_encode().returning(|_| Ok(vec![]));

            b.add_value(embeddings_chunker);
            b.bind::<dyn EmbeddingsChunker, MockEmbeddingsChunker>();
            b.add_value(embeddings_encoder);
            b.bind::<dyn EmbeddingsEncoder, MockEmbeddingsEncoder>();

            register_message_dispatcher::<AccountLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
            );

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                maybe_base_catalog: Some(&indexing_catalog),
                tenancy_config,
                outbox_provider: OutboxProvider::Dispatching,
                system_time_source_provider: SystemTimeSourceProvider::Inherited,
                ..Default::default()
            })
            .await;

        let no_subject_catalog = dataset_base_use_case_harness.no_subject_catalog();
        let system_user_catalog = dataset_base_use_case_harness.intermediate_catalog();

        // Ensure search indexes exist: this is not a normal startup path,
        //  but tests need it for "predefined" content
        let search_indexer = system_user_catalog.get_one::<SearchIndexerImpl>().unwrap();
        search_indexer.ensure_indexes_exist().await.unwrap();

        // Run predefined accounts registration, if specified
        if let Some(predefined_accounts_config) = predefined_accounts_config {
            let catalog = {
                let mut b = dill::CatalogBuilder::new_chained(no_subject_catalog);
                b.add_value(predefined_accounts_config);
                b.add::<PredefinedAccountsRegistrator>();
                b.add::<LoginPasswordAuthProvider>();
                b.add::<RebacServiceImpl>();
                b.add::<InMemoryRebacRepository>();
                b.add::<CreateAccountUseCaseImpl>();
                b.add::<UpdateAccountUseCaseImpl>();
                b.add_value(DefaultAccountProperties::default());
                b.add_value(DefaultDatasetProperties::default());
                b.build()
            };

            let predefined_accounts_registrator =
                catalog.get_one::<PredefinedAccountsRegistrator>().unwrap();
            predefined_accounts_registrator
                .run_initialization()
                .await
                .unwrap();
        }

        // Run predefined datasets registration, if specified
        if let Some(predefined_datasets_config) = predefined_datasets_config {
            let catalog = {
                let mut b = dill::CatalogBuilder::new_chained(no_subject_catalog);
                b.add::<CreateDatasetUseCaseImpl>();
                b.add::<CreateDatasetUseCaseHelper>();
                b.add_value(predefined_datasets_config);
                b.add::<PredefinedDatasetsRegistrator>();
                b.build()
            };

            let predefined_datasets_registrator =
                catalog.get_one::<PredefinedDatasetsRegistrator>().unwrap();
            predefined_datasets_registrator
                .run_initialization()
                .await
                .unwrap();
        }

        // Initialize outbox agent
        let outbox_agent = system_user_catalog.get_one::<dyn OutboxAgent>().unwrap();
        outbox_agent.run_initialization().await.unwrap();

        // Ensure search indexes are up to date after predefined datasets creation
        ElasticsearchBaseHarness::run_initial_indexing(system_user_catalog).await;

        Self {
            es_base_harness,
            dataset_base_use_case_harness,
            outbox_agent,
            fixed_time,
        }
    }

    #[inline]
    pub fn fixed_time(&self) -> DateTime<Utc> {
        self.fixed_time
    }

    #[inline]
    pub fn system_user_catalog(&self) -> &dill::Catalog {
        self.dataset_base_use_case_harness.intermediate_catalog()
    }

    #[inline]
    pub fn no_subject_catalog(&self) -> &dill::Catalog {
        self.dataset_base_use_case_harness.no_subject_catalog()
    }

    #[inline]
    pub fn search_repo(&self) -> &dyn SearchRepository {
        self.es_base_harness.es_ctx().search_repo()
    }

    pub async fn create_mt_datasets(
        &self,
        account_dataset_names: &[(&str, &str)],
    ) -> Vec<odf::DatasetID> {
        let mut dataset_ids = Vec::with_capacity(account_dataset_names.len());
        for (account_name, dataset_name) in account_dataset_names {
            let account_name = odf::AccountName::new_unchecked(account_name);

            let user_specific_catalog = {
                let mut b = dill::CatalogBuilder::new_chained(self.no_subject_catalog());
                b.add_value(CurrentAccountSubject::new_test_with(&account_name));
                b.build()
            };

            let alias = odf::DatasetAlias::new(
                Some(account_name.clone()),
                odf::DatasetName::new_unchecked(dataset_name),
            );
            dataset_ids.push(
                self.create_root_dataset(&user_specific_catalog, &alias)
                    .await
                    .dataset_handle
                    .id,
            );
        }

        dataset_ids
    }

    pub async fn process_outbox_messages(&self) {
        self.outbox_agent.run_while_has_tasks().await.unwrap();
    }

    pub async fn synchronize(&self) {
        self.process_outbox_messages().await;
        self.es_base_harness.es_ctx().refresh_indices().await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct PredefinedDatasetsConfig {
    pub aliases: Vec<odf::DatasetAlias>,
}

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.datasets.test.PredefinedDatasetsRegistrator",
    depends_on: &[],
    requires_transaction: true,
})]
struct PredefinedDatasetsRegistrator {
    time_source: Arc<dyn SystemTimeSource>,
    config: Arc<PredefinedDatasetsConfig>,
    catalog: dill::Catalog,
}

#[async_trait::async_trait]
impl InitOnStartup for PredefinedDatasetsRegistrator {
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // Create predefined datasets in silense
        for alias in &self.config.aliases {
            let user_specific_catalog = {
                let mut b = dill::CatalogBuilder::new_chained(&self.catalog);
                if let Some(account_name) = &alias.account_name {
                    b.add_value(CurrentAccountSubject::new_test_with(&account_name));
                } else {
                    b.add_value(CurrentAccountSubject::new_test());
                }
                b.build()
            };

            let create_dataset_use_case = user_specific_catalog
                .get_one::<dyn CreateDatasetUseCase>()
                .unwrap();

            create_dataset_use_case
                .execute(
                    alias,
                    MetadataFactory::metadata_block(
                        MetadataFactory::seed(odf::DatasetKind::Root)
                            .id(odf::DatasetID::new_seeded_ed25519(
                                alias.dataset_name.as_str().as_bytes(),
                            ))
                            .build(),
                    )
                    .system_time(self.time_source.now())
                    .build_typed(),
                    CreateDatasetUseCaseOptions {
                        dataset_visibility: odf::DatasetVisibility::Public,
                    },
                )
                .await
                .unwrap();
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
