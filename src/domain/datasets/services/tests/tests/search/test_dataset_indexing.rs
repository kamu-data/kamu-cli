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
use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME_STR};
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_datasets_services::*;
use kamu_search::*;
use kamu_search_elasticsearch::testing::{
    ElasticsearchBaseHarness,
    ElasticsearchTestContext,
    SearchTestResponse,
};
use time_source::SystemTimeSourceProvider;

use crate::tests::use_cases::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_dataset_index_initially_empty(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx).await;

    let dataset_index_response = harness.view_datasets_index().await;
    assert_eq!(dataset_index_response.total_hits(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_creating_datasets_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::new(ctx).await;

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
    let harness = DatasetIndexingHarness::new(ctx).await;

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
    let harness = DatasetIndexingHarness::new(ctx).await;

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
    pub async fn new(ctx: Arc<ElasticsearchTestContext>) -> Self {
        let es_base_harness = ElasticsearchBaseHarness::new(ctx);

        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                maybe_base_catalog: Some(es_base_harness.catalog()),
                tenancy_config: TenancyConfig::SingleTenant,
                system_time_source_provider: SystemTimeSourceProvider::Inherited,
                ..Default::default()
            })
            .await;

        let mut b =
            dill::CatalogBuilder::new_chained(dataset_base_use_case_harness.intermediate_catalog());
        b.add::<DatasetSearchSchemaProvider>()
            .add::<DatasetSearchUpdater>();

        let catalog = b.build();

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
