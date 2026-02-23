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
use kamu_accounts::PredefinedAccountsConfig;
use kamu_core::TenancyConfig;
use kamu_datasets::dataset_search_schema;
use kamu_search::*;
use kamu_search_elasticsearch::testing::{ElasticsearchTestContext, SearchTestResponse};

use super::es_dataset_base_harness::ElasticsearchDatasetBaseHarness;
use crate::tests::search::es_dataset_base_harness::{
    PrecomputedEmbeddingsConfig,
    PredefinedDatasetsConfig,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ElasticsearchDatasetBaseHarness, es_dataset_base_use_case_harness)]
pub struct DatasetSearchHarness {
    es_dataset_base_use_case_harness: ElasticsearchDatasetBaseHarness,
}

#[bon]
impl DatasetSearchHarness {
    #[builder]
    pub async fn new(
        ctx: Arc<ElasticsearchTestContext>,
        tenancy_config: TenancyConfig,
        maybe_predefined_accounts_config: Option<PredefinedAccountsConfig>,
        maybe_predefined_datasets_config: Option<PredefinedDatasetsConfig>,
        precomputed_embeddings_config: Option<PrecomputedEmbeddingsConfig>,
    ) -> Self {
        let es_dataset_base_use_case_harness =
            if let Some(precomputed_embeddings_config) = precomputed_embeddings_config {
                ElasticsearchDatasetBaseHarness::builder()
                    .ctx(ctx)
                    .tenancy_config(tenancy_config)
                    .maybe_predefined_accounts_config(maybe_predefined_accounts_config)
                    .maybe_predefined_datasets_config(maybe_predefined_datasets_config)
                    .precomputed_embeddings_config(precomputed_embeddings_config)
                    .build()
                    .await
            } else {
                ElasticsearchDatasetBaseHarness::builder()
                    .ctx(ctx)
                    .tenancy_config(tenancy_config)
                    .maybe_predefined_accounts_config(maybe_predefined_accounts_config)
                    .maybe_predefined_datasets_config(maybe_predefined_datasets_config)
                    .build()
                    .await
            };

        Self {
            es_dataset_base_use_case_harness,
        }
    }

    pub async fn search_dataset(&self, query: &str) -> SearchTestResponse {
        self.synchronize().await;

        let search_response = self
            .search_repo()
            .text_search(
                SearchSecurityContext::Unrestricted,
                TextSearchRequest {
                    intent: TextSearchIntent::make_full_text(query),
                    entity_schemas: vec![dataset_search_schema::SCHEMA_NAME],
                    source: SearchRequestSourceSpec::None,
                    filter: None,
                    secondary_sort: sort!(kamu_search::fields::TITLE),
                    page: SearchPaginationSpec {
                        limit: 100,
                        offset: 0,
                    },
                    options: TextSearchOptions::default(),
                },
            )
            .await
            .unwrap();

        SearchTestResponse(search_response)
    }

    pub async fn vector_search_dataset(
        &self,
        prompt: &str,
        filter: Option<SearchFilterExpr>,
        limit: usize,
    ) -> SearchTestResponse {
        self.synchronize().await;

        let embeddings_provider = self
            .system_user_catalog()
            .get_one::<dyn EmbeddingsProvider>()
            .unwrap();

        let prompt_embedding = embeddings_provider
            .provide_prompt_embeddings(prompt.to_string())
            .await
            .unwrap();

        let search_response = self
            .search_repo()
            .vector_search(
                SearchSecurityContext::Unrestricted,
                VectorSearchRequest {
                    prompt_embedding,
                    entity_schemas: vec![dataset_search_schema::SCHEMA_NAME],
                    source: SearchRequestSourceSpec::None,
                    filter,
                    limit,
                    options: VectorSearchOptions {
                        knn: KnnOptions::for_limit(limit),
                        enable_explain: false,
                    },
                },
            )
            .await
            .unwrap();

        SearchTestResponse(search_response)
    }

    pub async fn vector_search_dataset_by_prompt(&self, prompt: &str) -> SearchTestResponse {
        self.vector_search_dataset(prompt, None, 100).await
    }

    pub async fn hybrid_search_dataset(
        &self,
        prompt: &str,
        filter: Option<SearchFilterExpr>,
        limit: usize,
    ) -> SearchTestResponse {
        self.synchronize().await;

        let embeddings_provider = self
            .system_user_catalog()
            .get_one::<dyn EmbeddingsProvider>()
            .unwrap();

        let prompt_embedding = embeddings_provider
            .provide_prompt_embeddings(prompt.to_string())
            .await
            .unwrap();

        let search_response = self
            .search_repo()
            .hybrid_search(
                SearchSecurityContext::Unrestricted,
                HybridSearchRequest {
                    prompt: prompt.to_string(),
                    prompt_embedding,
                    entity_schemas: vec![dataset_search_schema::SCHEMA_NAME],
                    source: SearchRequestSourceSpec::None,
                    filter,
                    secondary_sort: vec![],
                    limit,
                    options: HybridSearchOptions::for_limit(limit),
                },
            )
            .await
            .unwrap();

        SearchTestResponse(search_response)
    }

    pub async fn hybrid_search_dataset_by_prompt(&self, prompt: &str) -> SearchTestResponse {
        self.hybrid_search_dataset(prompt, None, 100).await
    }

    pub async fn view_datasets_index_as(&self, account_id: &odf::AccountID) -> SearchTestResponse {
        self.synchronize().await;

        let search_response = self
            .search_repo()
            .listing_search(
                SearchSecurityContext::Restricted {
                    current_principal_ids: vec![account_id.to_string()],
                },
                ListingSearchRequest {
                    entity_schemas: vec![dataset_search_schema::SCHEMA_NAME],
                    source: SearchRequestSourceSpec::All,
                    filter: None,
                    sort: sort!(dataset_search_schema::fields::DATASET_NAME),
                    page: SearchPaginationSpec {
                        limit: 100,
                        offset: 0,
                    },
                },
            )
            .await
            .unwrap();

        SearchTestResponse(search_response)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
