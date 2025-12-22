// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::*;
use kamu_search::*;
use url::Url;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lazily spawns a local `ElasticSearch` container
/// that will be cleaned up on exit
pub struct ElasticSearchContainerRepository {
    runtime: Arc<container_runtime::ContainerRuntime>,
    config: Arc<ElasticSearchContainerConfig>,
    state: tokio::sync::OnceCell<State>,
}

#[allow(dead_code)]
struct State {
    container: container_runtime::ContainerProcess,
    inner: ElasticSearchRepository,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn SearchRepository)]
impl ElasticSearchContainerRepository {
    pub fn new(
        runtime: Arc<container_runtime::ContainerRuntime>,
        config: Arc<ElasticSearchContainerConfig>,
    ) -> Self {
        Self {
            runtime,
            config,
            state: tokio::sync::OnceCell::new(),
        }
    }

    async fn inner(&self) -> Result<&ElasticSearchRepository, InternalError> {
        let state = self
            .state
            .get_or_try_init(async || self.init_state().await)
            .await?;

        Ok(&state.inner)
    }

    async fn init_state(&self) -> Result<State, InternalError> {
        const DUMMY_PASSWORD: &str = "root";

        let container = self
            .runtime
            .run_attached(self.config.image.clone())
            .random_container_name_with_prefix("kamu-search-elasticsearch-")
            .expose_ports([9200])
            .environment_vars([
                ("ES_JAVA_OPTS", "-Xms1024m -Xmx1024m"),
                ("xpack.security.enabled", "false"),
                ("xpack.security.http.ssl.enabled", "false"),
                ("xpack.security.transport.ssl.enabled", "false"),
                ("xpack.ml.enabled", "false"),
                ("discovery.type", "single-node"),
                ("cluster.routing.allocation.disk.threshold_enabled", "false"),
                ("ELASTIC_PASSWORD", DUMMY_PASSWORD),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .int_err()?;

        let runtime_host = self.runtime.get_runtime_host_addr();
        let rest_api_port = container
            .wait_for_host_socket(9200, self.config.start_timeout)
            .await
            .int_err()?;

        let url = Url::parse(&format!("http://{runtime_host}:{rest_api_port}")).int_err()?;
        tracing::info!("ElasticSearch container is starting at {url}");

        let inner = ElasticSearchRepository::new(Arc::new(ElasticSearchConfig {
            url,
            password: Some(DUMMY_PASSWORD.to_string()),
            timeout_secs: 5,
            enable_compression: false,
            index_prefix: String::new(),
        }));

        Ok(State { container, inner })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SearchRepository for ElasticSearchContainerRepository {
    async fn health(&self) -> Result<serde_json::Value, InternalError> {
        self.inner().await?.health().await
    }

    async fn search(&self, req: SearchRequest) -> Result<SearchResponse, InternalError> {
        self.inner().await?.search(req).await
    }

    async fn ensure_entity_index(
        &self,
        schema: &SearchEntitySchema,
    ) -> Result<(), SearchEnsureEntityIndexError> {
        self.inner().await?.ensure_entity_index(schema).await
    }

    async fn total_documents(&self) -> Result<u64, InternalError> {
        self.inner().await?.total_documents().await
    }

    async fn documents_of_kind(
        &self,
        schema_name: SearchEntitySchemaName,
    ) -> Result<u64, InternalError> {
        self.inner().await?.documents_of_kind(schema_name).await
    }

    async fn find_document_by_id(
        &self,
        schema_name: SearchEntitySchemaName,
        id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError> {
        self.inner()
            .await?
            .find_document_by_id(schema_name, id)
            .await
    }

    async fn bulk_update(
        &self,
        schema_name: SearchEntitySchemaName,
        operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), InternalError> {
        self.inner()
            .await?
            .bulk_update(schema_name, operations)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
