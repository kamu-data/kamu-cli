// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_search::SearchRepository;
use random_strings::get_random_name;
use tokio::sync::OnceCell;
use url::Url;

use crate::es_client::ElasticsearchClient;
use crate::{ElasticsearchClientConfig, ElasticsearchRepository, ElasticsearchRepositoryConfig};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static ELASTICSEARCH_CLIENT: OnceCell<Arc<ElasticsearchClient>> = OnceCell::const_new();

const ENV_ELASTICSEARCH_URL: &str = "ELASTICSEARCH_URL";
const ENV_ELASTICSEARCH_PASSWORD: &str = "ELASTICSEARCH_PASSWORD";

const DEFAULT_ELASTICSEARCH_URL: &str = "http://localhost:9200";
const DEFAULT_ELASTICSEARCH_PASSWORD: Option<&str> = None;

const INDEX_PREFIX_TEMPLATE: &str = "kamu-test-";

const ELASTICSEARCH_TIMEOUT_SECS: u64 = 180;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticsearchTestContext {
    catalog: dill::Catalog,
    client: Arc<ElasticsearchClient>,
    search_repo: Arc<ElasticsearchRepository>,
    index_prefix: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ElasticsearchTestContext {
    pub async fn new(_test_name: &str) -> Self {
        // Read configuration from environment variables
        let es_url = std::env::var(ENV_ELASTICSEARCH_URL)
            .unwrap_or_else(|_| DEFAULT_ELASTICSEARCH_URL.to_string());
        let es_password = std::env::var(ENV_ELASTICSEARCH_PASSWORD)
            .ok()
            .or_else(|| DEFAULT_ELASTICSEARCH_PASSWORD.map(ToString::to_string));

        // Client config
        let client_config = ElasticsearchClientConfig {
            url: Url::parse(&es_url).unwrap(),
            password: es_password.clone(),
            timeout_secs: ELASTICSEARCH_TIMEOUT_SECS,
            enable_compression: false,
        };

        // Reuse client across tests to speed up execution
        // (helps with `cargo test`, but not with `argo nextest run`)
        let client = ELASTICSEARCH_CLIENT
            .get_or_init(|| async {
                // Initialize client
                let client = ElasticsearchClient::init(&client_config).unwrap();
                Arc::new(client)
            })
            .await
            .clone();

        // Prepare repository config: this one is test-specific and not shared
        let index_prefix = get_random_name(Some(INDEX_PREFIX_TEMPLATE), 10).to_ascii_lowercase();
        let repo_config = ElasticsearchRepositoryConfig {
            index_prefix: index_prefix.clone(),
        };

        // Manually build repository with predefined client and config
        let mut catalog_builder = dill::CatalogBuilder::new();
        catalog_builder.add_value(ElasticsearchRepository::with_predefined_client(
            Arc::new(client_config),
            Arc::new(repo_config),
            client.clone(),
        ));
        catalog_builder.bind::<dyn SearchRepository, ElasticsearchRepository>();

        let catalog = catalog_builder.build();
        let search_repo = catalog.get_one::<ElasticsearchRepository>().unwrap();

        Self {
            catalog,
            client,
            search_repo,
            index_prefix,
        }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub fn index_prefix(&self) -> &str {
        &self.index_prefix
    }

    pub fn search_repo(&self) -> &dyn SearchRepository {
        self.search_repo.as_ref()
    }

    pub async fn refresh_indices(&self) {
        // List all indices with the test prefix
        let test_index_names = self
            .client
            .list_indices_by_prefix(&self.index_prefix)
            .await
            .unwrap();

        // Convert to refs
        let refs: Vec<&str> = test_index_names.iter().map(String::as_str).collect();

        // Refresh them - this is potentially a long waiting
        self.client.refresh_indices(&refs).await.unwrap();
    }

    pub async fn cleanup(self: Arc<Self>) {
        // List all indices with the test prefix
        let test_index_names = self
            .client
            .list_indices_by_prefix(&self.index_prefix)
            .await
            .unwrap();

        // Convert to refs
        let refs: Vec<&str> = test_index_names.iter().map(String::as_str).collect();

        // Delete them
        let _ = self.client.delete_indices_bulk(&refs).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn elasticsearch_test<T, Fut, E>(test_name: &str, f: T) -> Result<(), E>
where
    T: FnOnce(Arc<ElasticsearchTestContext>) -> Fut,
    Fut: std::future::Future<Output = Result<(), E>>,
{
    // Initialize context
    let ctx = Arc::new(ElasticsearchTestContext::new(test_name).await);

    // Execute test body
    let res = f(ctx.clone()).await;

    // Auto-clean in case of success,
    // but keep the context for inspection in case of failure
    if res.is_ok() {
        ctx.cleanup().await;
    }

    // Propagate the result
    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SearchTestResponse(pub kamu_search::SearchResponse);

impl SearchTestResponse {
    pub fn total_hits(&self) -> u64 {
        self.0.total_hits
    }

    pub fn ids(&self) -> Vec<kamu_search::SearchEntityId> {
        self.0.hits.iter().map(|hit| hit.id.clone()).collect()
    }

    pub fn entities(&self) -> Vec<serde_json::Value> {
        self.0.hits.iter().map(|hit| hit.source.clone()).collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
