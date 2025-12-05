// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bon::bon;
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use kamu::testing::MockDatasetActionAuthorizer;
use kamu_accounts::{CurrentAccountSubject, LoggedAccount};
use kamu_accounts_inmem::{InMemoryAccountQuotaEventStore, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::{
    AccountQuotaServiceImpl,
    CreateAccountUseCaseImpl,
    QuotaCheckerStorageImpl,
    UpdateAccountUseCaseImpl,
};
use kamu_adapter_graphql::data_loader::{account_entity_data_loader, dataset_handle_data_loader};
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::{DidGeneratorDefault, RunInfoDir, TenancyConfig};
use kamu_datasets::*;
use kamu_datasets_inmem::{InMemoryDatasetStatisticsRepository, *};
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::{DatasetStatisticsServiceImpl, *};
use messaging_outbox::*;
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct BaseGQLDatasetHarness {
    tempdir: TempDir,
    catalog: dill::Catalog,
    schema: kamu_adapter_graphql::Schema,
}

#[bon]
impl BaseGQLDatasetHarness {
    #[builder]
    pub fn new(
        tenancy_config: TenancyConfig,
        mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
    ) -> Self {
        use dill::Component;

        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let run_info_dir = tempdir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add_value(kamu_adapter_graphql::Config::default())
                .add_builder(
                    OutboxImmediateImpl::builder()
                        .with_consumer_filter(ConsumerFilter::AllConsumers),
                )
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add::<DidGeneratorDefault>()
                .add_value(tenancy_config)
                .add::<DatabaseTransactionRunner>()
                .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                    datasets_dir,
                ))
                .add::<DatasetLfsBuilderDatabaseBackedImpl>()
                .add_value(kamu_datasets_services::MetadataChainDbBackedConfig::default())
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<CreateDatasetUseCaseImpl>()
                .add::<UpdateAccountUseCaseImpl>()
                .add::<CreateAccountUseCaseImpl>()
                .add::<CreateDatasetUseCaseHelper>()
                .add::<SystemTimeSourceDefault>()
                .add::<DatasetReferenceServiceImpl>()
                .add::<InMemoryDatasetReferenceRepository>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add::<DependencyGraphImmediateListener>()
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>()
                .add::<RebacDatasetRegistryFacadeImpl>()
                .add::<InMemoryDatasetKeyBlockRepository>()
                .add::<InMemoryDatasetDataBlockRepository>()
                .add::<InMemoryDidSecretKeyRepository>()
                .add::<DatasetBlockUpdateHandler>()
                .add::<InMemoryAccountQuotaEventStore>()
                .add::<AccountQuotaServiceImpl>()
                .add::<InMemoryDatasetStatisticsRepository>()
                .add::<DatasetStatisticsServiceImpl>()
                .add::<QuotaCheckerStorageImpl>()
                .add_value(RunInfoDir::new(run_info_dir));

            if let Some(mock) = mock_dataset_action_authorizer {
                b.add_value(mock)
                    .bind::<dyn DatasetActionAuthorizer, MockDatasetActionAuthorizer>();
            } else {
                kamu_adapter_auth_oso_rebac::register_dependencies(&mut b);
            }

            NoOpDatabasePlugin::init_database_components(&mut b);

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

        Self {
            tempdir,
            catalog,
            schema: kamu_adapter_graphql::schema_quiet(),
        }
    }

    pub fn temp_dir(&self) -> &std::path::Path {
        self.tempdir.path()
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    #[expect(dead_code)]
    pub fn schema(&self) -> &kamu_adapter_graphql::Schema {
        &self.schema
    }

    pub fn logged_account_from_catalog(&self, catalog: &dill::Catalog) -> LoggedAccount {
        let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
        if let CurrentAccountSubject::Logged(logged) = current_account_subject.as_ref() {
            logged.clone()
        } else {
            panic!("Expected logged current user");
        }
    }

    pub async fn execute_query(
        &self,
        query: impl Into<async_graphql::Request>,
        catalog: &dill::Catalog,
    ) -> async_graphql::Response {
        self.schema
            .execute(
                query
                    .into()
                    .data(account_entity_data_loader(catalog))
                    .data(dataset_handle_data_loader(catalog))
                    .data(catalog.clone()),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
