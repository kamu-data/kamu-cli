// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use dill::Component;
use kamu_accounts::{
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME_STR,
    DidSecretEncryptionConfig,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::AccountServiceImpl;
use kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl;
use kamu_core::auth::AlwaysHappyDatasetActionAuthorizer;
use kamu_core::{DidGeneratorDefault, TenancyConfig};
use kamu_datasets::*;
use kamu_datasets_inmem::{
    InMemoryDatasetDataBlockRepository,
    InMemoryDatasetDependencyRepository,
    InMemoryDatasetEntryRepository,
    InMemoryDatasetKeyBlockRepository,
    InMemoryDatasetReferenceRepository,
};
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use kamu_search::*;
use kamu_search_elasticsearch::testing::{EsTestContext, SearchTestResponse};
use kamu_search_services::{SearchIndexer, SearchServiceImpl};
use messaging_outbox::{Outbox, OutboxImmediateImpl, register_message_dispatcher};
use time_source::SystemTimeSourceStub;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_dataset_index_initially_empty(ctx: Arc<EsTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx).await;

    let dataset_index_response = harness.view_datasets_index().await;
    assert_eq!(dataset_index_response.total_hits(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_creating_datasets_reflected_in_index(ctx: Arc<EsTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx).await;

    let dataset_names = vec!["alpha", "beta", "gamma"];
    let mut dataset_ids = Vec::with_capacity(dataset_names.len());
    for dataset_name in &dataset_names {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name));
        dataset_ids.push(harness.create_root_dataset(&alias).await.dataset_handle.id);
    }

    let datasets_index_response = harness.view_datasets_index().await;

    assert_eq!(datasets_index_response.total_hits(), 3);

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        dataset_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "beta",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time.to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_renaming_datasets_reflected_in_index(ctx: Arc<EsTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx).await;

    let dataset_names = vec!["alpha", "beta", "gamma"];
    let mut dataset_ids = Vec::with_capacity(dataset_names.len());
    for dataset_name in &dataset_names {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name));
        dataset_ids.push(harness.create_root_dataset(&alias).await.dataset_handle.id);
    }

    harness
        .rename_dataset(&dataset_ids[1], "beta-renamed")
        .await;
    harness.rename_dataset(&dataset_ids[0], "test-alpha").await;

    let datasets_index_response = harness.view_datasets_index().await;

    assert_eq!(datasets_index_response.total_hits(), 3);

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        vec![
            // Order changed due to renaming of "alpha" to "test-alpha"
            dataset_ids[1].to_string(),
            dataset_ids[2].to_string(),
            dataset_ids[0].to_string()
        ]
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "beta-renamed",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta-renamed",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "test-alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "test-alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time.to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_deleting_datasets_reflected_in_index(ctx: Arc<EsTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx).await;

    let dataset_names = vec!["alpha", "beta", "gamma"];
    let mut dataset_ids = Vec::with_capacity(dataset_names.len());
    for dataset_name in &dataset_names {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name));
        dataset_ids.push(harness.create_root_dataset(&alias).await.dataset_handle.id);
    }

    // Delete "beta" dataset
    harness.delete_dataset(&dataset_ids[1]).await;

    let datasets_index_response = harness.view_datasets_index().await;

    assert_eq!(datasets_index_response.total_hits(), 2);

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        vec![dataset_ids[0].to_string(), dataset_ids[2].to_string(),]
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time.to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetIndexingHarness {
    _tempdir: tempfile::TempDir,
    fixed_time: chrono::DateTime<Utc>,
    ctx: Arc<EsTestContext>,
    catalog: dill::Catalog,
}

impl DatasetIndexingHarness {
    pub async fn new(ctx: Arc<EsTestContext>) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let mut b = dill::CatalogBuilder::new_chained(ctx.catalog());

        use odf::dataset::DatasetStorageUnitLocalFs;
        b.add_builder(DatasetStorageUnitLocalFs::builder(datasets_dir));
        b.add::<DatasetLfsBuilderDatabaseBackedImpl>();
        b.add_value(MetadataChainDbBackedConfig::default());

        b.add::<DatasetSearchSchemaProvider>()
            .add::<DatasetSearchUpdater>()
            .add::<DatasetEntryServiceImpl>()
            .add::<InMemoryDatasetEntryRepository>()
            .add::<InMemoryDidSecretKeyRepository>()
            .add::<InMemoryDatasetKeyBlockRepository>()
            .add::<InMemoryDatasetDataBlockRepository>()
            .add::<InMemoryDatasetReferenceRepository>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<DatasetReferenceServiceImpl>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<CreateDatasetUseCaseHelper>()
            .add::<DeleteDatasetUseCaseImpl>()
            .add::<RenameDatasetUseCaseImpl>()
            .add::<DidGeneratorDefault>()
            .add_value(DidSecretEncryptionConfig::sample())
            .add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add::<RebacDatasetRegistryFacadeImpl>()
            .add::<AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceImpl>()
            .add::<SearchIndexer>()
            .add::<SearchServiceImpl>()
            .add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<SystemTimeSourceStub>();

        register_message_dispatcher::<DatasetLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );

        register_message_dispatcher::<DatasetReferenceMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
        );

        let catalog = b.build();

        let fixed_time = Utc::now();
        let time_source = catalog.get_one::<SystemTimeSourceStub>().unwrap();
        time_source.set(fixed_time);

        use init_on_startup::InitOnStartup;
        let indexer = catalog.get_one::<SearchIndexer>().unwrap();
        indexer.run_initialization().await.unwrap();

        Self {
            _tempdir: tempdir,
            fixed_time,
            ctx,
            catalog,
        }
    }

    pub async fn create_root_dataset(&self, alias: &odf::DatasetAlias) -> CreateDatasetResult {
        let use_case = self
            .catalog
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

    pub async fn rename_dataset(&self, dataset_id: &odf::DatasetID, new_name: &str) {
        let use_case = self.catalog.get_one::<dyn RenameDatasetUseCase>().unwrap();

        use_case
            .execute(
                &dataset_id.as_local_ref(),
                &odf::DatasetName::new_unchecked(new_name),
            )
            .await
            .unwrap();
    }

    pub async fn delete_dataset(&self, dataset_id: &odf::DatasetID) {
        let use_case = self.catalog.get_one::<dyn DeleteDatasetUseCase>().unwrap();

        use_case
            .execute_via_ref(&dataset_id.as_local_ref())
            .await
            .unwrap();
    }

    pub async fn view_datasets_index(&self) -> SearchTestResponse {
        self.ctx.refresh_indices().await;

        let search_repo = self.ctx.search_repo();

        let seach_response = search_repo
            .search(SearchRequest {
                query: None,
                entity_schemas: vec![dataset_search_schema::SCHEMA_NAME],
                source: SearchRequestSourceSpec::All,
                filter: None,
                sort: sort!(dataset_search_schema::fields::DATASET_NAME),
                page: SearchPaginationSpec {
                    limit: 100,
                    offset: 0,
                },
                options: SearchOptions::default(),
            })
            .await
            .unwrap();

        SearchTestResponse(seach_response)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
