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

// NOTE: We cache only the parsed config (which involves URL parsing, env
// lookups and CA cert path resolution + file read), NOT the
// `ElasticsearchClient` itself. `reqwest::Client` wraps a `hyper` connection
// pool whose dispatcher task is spawned on whichever Tokio runtime called
// `Client::build()`. Reusing it across different `#[tokio::test]` runtimes
// causes `DispatchGone` ("runtime dropped the dispatch task") errors once the
// originating runtime shuts down.
static ELASTICSEARCH_CLIENT_CONFIG: OnceCell<Arc<ElasticsearchClientConfig>> =
    OnceCell::const_new();

const ENV_ELASTICSEARCH_URL: &str = "ELASTICSEARCH_URL";
const ENV_ELASTICSEARCH_PASSWORD: &str = "ELASTICSEARCH_PASSWORD";
const ENV_ELASTICSEARCH_CA_CERT_PEM_PATH: &str = "ELASTICSEARCH_CA_CERT_PEM_PATH";

const DEFAULT_ELASTICSEARCH_URL: &str = "http://localhost:9200";
const DEFAULT_ELASTICSEARCH_PASSWORD: Option<&str> = Some("root");

const ELASTICSEARCH_HTTPS_CA_CERT_PEM_PATH: &str = ".local/elasticsearch/certs/ca/ca.crt";

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
        // Reuse configuration across tests to avoid repeated env lookups,
        // URL parsing, and CA cert path resolution.
        let client_config = ELASTICSEARCH_CLIENT_CONFIG
            .get_or_init(|| async { Arc::new(Self::load_client_config()) })
            .await
            .clone();

        // Build a fresh client per test. `reqwest::Client` binds its
        // connection-pool dispatcher to the current Tokio runtime, so sharing
        // one across `#[tokio::test]` runtimes causes `DispatchGone` failures.
        let client = Arc::new(ElasticsearchClient::init(&client_config).unwrap());

        // Prepare repository config: this one is test-specific and not shared
        let index_prefix = get_random_name(Some(INDEX_PREFIX_TEMPLATE), 10).to_ascii_lowercase();
        let repo_config = ElasticsearchRepositoryConfig {
            index_prefix: index_prefix.clone(),
            embedding_dimensions: 1536,
        };

        // Manually build repository with predefined client and config
        let mut catalog_builder = dill::CatalogBuilder::new();
        catalog_builder.add_value(ElasticsearchRepository::with_predefined_client(
            client_config,
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

    fn load_client_config() -> ElasticsearchClientConfig {
        // Read configuration from environment variables
        let es_url = std::env::var(ENV_ELASTICSEARCH_URL)
            .unwrap_or_else(|_| DEFAULT_ELASTICSEARCH_URL.to_string());
        let es_url = Url::parse(&es_url).unwrap();

        let es_password = std::env::var(ENV_ELASTICSEARCH_PASSWORD)
            .ok()
            .or_else(|| DEFAULT_ELASTICSEARCH_PASSWORD.map(ToString::to_string));

        // Certificates are only used for HTTPS connections
        let es_ca_cert_pem_path = std::env::var(ENV_ELASTICSEARCH_CA_CERT_PEM_PATH)
            .ok()
            .or_else(|| {
                // Only use default certificate path for HTTPS connections
                if es_url.scheme() == "https" {
                    Some(ELASTICSEARCH_HTTPS_CA_CERT_PEM_PATH.to_string())
                } else {
                    None
                }
            })
            .map(|path| {
                let path_buf = std::path::PathBuf::from(&path);
                if path_buf.is_absolute() {
                    path_buf
                } else {
                    // Walk up from current directory until we find the file
                    let mut current = std::env::current_dir().unwrap();
                    loop {
                        let candidate = current.join(&path_buf);
                        if candidate.exists() {
                            return candidate;
                        }
                        assert!(current.pop(), "Could not find certificate file: {path}");
                    }
                }
            });

        ElasticsearchClientConfig {
            url: es_url,
            password: es_password,
            ca_cert_pem_path: es_ca_cert_pem_path,
            timeout_secs: ELASTICSEARCH_TIMEOUT_SECS,
            enable_compression: false,
        }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    #[inline]
    pub fn client(&self) -> &ElasticsearchClient {
        self.client.as_ref()
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
    pub fn total_hits(&self) -> Option<u64> {
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
