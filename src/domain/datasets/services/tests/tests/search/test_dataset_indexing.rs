// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::NoOpDatabasePlugin;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::InternalError;
use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME_STR};
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use kamu_search::*;
use kamu_search_elasticsearch::testing::{
    ElasticsearchBaseHarness,
    ElasticsearchTestContext,
    SearchTestResponse,
};
use kamu_search_services::SearchIndexer;
use messaging_outbox::*;
use odf::metadata::testing::MetadataFactory;
use time_source::{SystemTimeSource, SystemTimeSourceProvider};

use crate::tests::use_cases::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_dataset_index_initially_empty(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx, None).await;

    let dataset_index_response = harness.view_datasets_index().await;
    assert_eq!(dataset_index_response.total_hits(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_predefined_datasets_indexed_properly(ctx: Arc<ElasticsearchTestContext>) {
    let aliases = vec![
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar")),
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo")),
    ];

    let predefined_datasets_config = PredefinedDatasetsConfig {
        aliases: aliases.clone(),
    };

    let harness = DatasetIndexingHarness::new(ctx, Some(predefined_datasets_config)).await;

    let dataset_index_response = harness.view_datasets_index().await;
    assert_eq!(dataset_index_response.total_hits(), 2);

    pretty_assertions::assert_eq!(
        dataset_index_response.ids(),
        aliases
            .iter()
            .map(|alias| {
                odf::DatasetID::new_seeded_ed25519(alias.dataset_name.as_str().as_bytes())
                    .to_string()
            })
            .collect::<Vec<_>>(),
    );

    pretty_assertions::assert_eq!(
        dataset_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "bar",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "bar",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "foo",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "foo",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            })
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_creating_datasets_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx, None).await;

    let dataset_names = vec!["alpha", "beta", "gamma"];
    let mut dataset_ids = Vec::with_capacity(dataset_names.len());
    for dataset_name in &dataset_names {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name));
        dataset_ids.push(
            harness
                .create_root_dataset(&harness.catalog, &alias)
                .await
                .dataset_handle
                .id,
        );
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
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "beta",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_renaming_datasets_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx, None).await;

    let dataset_names = vec!["alpha", "beta", "gamma"];
    let mut dataset_ids = Vec::with_capacity(dataset_names.len());
    for dataset_name in &dataset_names {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name));
        dataset_ids.push(
            harness
                .create_root_dataset(&harness.catalog, &alias)
                .await
                .dataset_handle
                .id,
        );
    }

    harness
        .rename_dataset(&harness.catalog, &dataset_ids[1], "beta-renamed")
        .await;
    harness
        .rename_dataset(&harness.catalog, &dataset_ids[0], "test-alpha")
        .await;

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
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta-renamed",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "test-alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "test-alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_deleting_datasets_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx, None).await;

    let dataset_names = vec!["alpha", "beta", "gamma"];
    let mut dataset_ids = Vec::with_capacity(dataset_names.len());
    for dataset_name in &dataset_names {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name));
        dataset_ids.push(
            harness
                .create_root_dataset(&harness.catalog, &alias)
                .await
                .dataset_handle
                .id,
        );
    }

    // Delete "beta" dataset
    harness
        .delete_dataset(&harness.catalog, &dataset_ids[1])
        .await;

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
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct DatasetIndexingHarness {
    es_base_harness: ElasticsearchBaseHarness,
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    catalog: dill::Catalog,
}

impl DatasetIndexingHarness {
    pub async fn new(
        ctx: Arc<ElasticsearchTestContext>,
        maybe_predefined_datasets: Option<PredefinedDatasetsConfig>,
    ) -> Self {
        let es_base_harness = ElasticsearchBaseHarness::new(ctx);

        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                maybe_base_catalog: Some(es_base_harness.catalog()),
                tenancy_config: TenancyConfig::SingleTenant,
                system_time_source_provider: SystemTimeSourceProvider::Inherited,
                outbox_provider: OutboxProvider::PauseableImmediate,
                ..Default::default()
            })
            .await;

        let mut b =
            dill::CatalogBuilder::new_chained(dataset_base_use_case_harness.intermediate_catalog());
        b.add::<DatasetSearchSchemaProvider>()
            .add::<DatasetSearchUpdater>();

        let run_predefined_datasets_registration = maybe_predefined_datasets.is_some();
        if let Some(predefined_datasets) = maybe_predefined_datasets {
            b.add::<CreateDatasetUseCaseImpl>();
            b.add::<CreateDatasetUseCaseHelper>();
            b.add::<PredefinedDatasetsRegistrator>();
            b.add_value(predefined_datasets);
        }

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        // Ensure search indexes exist: this is not a normal startup path,
        //  but tests need it for "predefined" content
        let search_indexer = catalog.get_one::<SearchIndexer>().unwrap();
        search_indexer.ensure_indexes_exist().await.unwrap();

        // Run predefined datasets registration, if specified
        if run_predefined_datasets_registration {
            let predefined_datasets_registrator =
                catalog.get_one::<PredefinedDatasetsRegistrator>().unwrap();
            predefined_datasets_registrator
                .run_initialization()
                .await
                .unwrap();
        }

        // Ensure search indexes are up to date after predefined datasets creation
        ElasticsearchBaseHarness::run_initial_indexing(&catalog).await;

        Self {
            es_base_harness,
            dataset_base_use_case_harness,
            catalog,
        }
    }

    #[inline]
    pub fn fixed_time(&self) -> DateTime<Utc> {
        self.es_base_harness.fixed_time()
    }

    pub async fn view_datasets_index(&self) -> SearchTestResponse {
        self.es_base_harness.es_ctx().refresh_indices().await;

        let search_repo = self.es_base_harness.es_ctx().search_repo();

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

#[derive(Clone)]
struct PredefinedDatasetsConfig {
    aliases: Vec<odf::DatasetAlias>,
}

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.datasets.test.PredefinedDatasetsRegistrator",
    depends_on: &[],
    requires_transaction: true,
})]
struct PredefinedDatasetsRegistrator {
    create_dataset_use_case: Arc<dyn CreateDatasetUseCase>,
    time_source: Arc<dyn SystemTimeSource>,
    config: Arc<PredefinedDatasetsConfig>,
    pausable_outbox: Arc<PausableImmediateOutboxImpl>,
}

#[async_trait::async_trait]
impl InitOnStartup for PredefinedDatasetsRegistrator {
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // Pause outbox notifications
        self.pausable_outbox.pause();

        // Create predefined datasets in silense
        for alias in &self.config.aliases {
            self.create_dataset_use_case
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

        // Resume outbox notifications
        self.pausable_outbox.resume();

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
