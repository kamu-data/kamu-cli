// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_search::*;

use crate::ElasticSearchFullTextSearchConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DEFAULT_ELASTICSEARCH_USER: &str = "elastic";
const DEFAULT_ELASTICSEARCH_PASSWORD: &str = "root";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticSearchFullTextRepo {
    config: Arc<ElasticSearchFullTextSearchConfig>,
    client: tokio::sync::OnceCell<elasticsearch::Elasticsearch>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn FullTextSearchRepository)]
impl ElasticSearchFullTextRepo {
    pub fn new(config: Arc<ElasticSearchFullTextSearchConfig>) -> Self {
        Self {
            config,
            client: tokio::sync::OnceCell::new(),
        }
    }

    fn es_client(
        config: &ElasticSearchFullTextSearchConfig,
    ) -> Result<elasticsearch::Elasticsearch, InternalError> {
        use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};

        let pool = SingleNodeConnectionPool::new(config.url.clone());

        let builder = TransportBuilder::new(pool)
            .disable_proxy() // often desirable in k8s
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .request_body_compression(config.enable_compression)
            .auth(elasticsearch::auth::Credentials::Basic(
                DEFAULT_ELASTICSEARCH_USER.into(),
                config
                    .password
                    .as_ref()
                    .map(|p| p.as_str().to_string())
                    .unwrap_or_else(|| DEFAULT_ELASTICSEARCH_PASSWORD.into()),
            ));

        let transport = builder.build().int_err()?;
        Ok(elasticsearch::Elasticsearch::new(transport))
    }

    fn full_index_name(&self, index_name: &str) -> String {
        if self.config.index_prefix.is_empty() {
            index_name.to_string()
        } else {
            format!("{}-{}", self.config.index_prefix, index_name)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl FullTextSearchRepository for ElasticSearchFullTextRepo {
    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_health skip_all)]
    async fn health(&self) -> Result<serde_json::Value, InternalError> {
        let client = self
            .client
            .get_or_try_init(async || Self::es_client(&self.config))
            .await?;

        let response = client
            .cluster()
            .health(elasticsearch::cluster::ClusterHealthParts::None)
            .send()
            .await
            .int_err()?;

        let body = response.json().await.int_err()?;
        Ok(body)
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_has_entity_index, skip_all)]
    async fn has_entity_index(&self, kind: &str) -> Result<bool, InternalError> {
        let client = self
            .client
            .get_or_try_init(async || Self::es_client(&self.config))
            .await?;

        let full_index_name = self.full_index_name(kind);

        let response = client
            .indices()
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[
                &full_index_name,
            ]))
            .send()
            .await
            .int_err()?;

        Ok(response.status_code().is_success())
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_create_entity_index, skip_all)]
    async fn create_entity_index(
        &self,
        entity_schema: &FullTextSearchEntitySchema,
    ) -> Result<(), InternalError> {
        let client = self
            .client
            .get_or_try_init(async || Self::es_client(&self.config))
            .await?;

        let full_index_name = self.full_index_name(entity_schema.entity_kind);

        let mut mappings = serde_json::Map::new();
        for field in entity_schema.fields {
            let field_mapping = match field.kind {
                FullTextSchemaFieldKind::Text => serde_json::json!({"type":"text"}),
                FullTextSchemaFieldKind::Keyword => serde_json::json!({"type":"keyword"}),
            };
            mappings.insert(field.path.to_string(), field_mapping);
        }

        let body = serde_json::json!({"mappings": {"properties": mappings}});

        client
            .indices()
            .create(elasticsearch::indices::IndicesCreateParts::Index(
                &full_index_name,
            ))
            .body(body)
            .send()
            .await
            .int_err()?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_total_documents, skip_all)]
    async fn total_documents(&self) -> Result<u64, InternalError> {
        let client = self
            .client
            .get_or_try_init(async || Self::es_client(&self.config))
            .await?;

        let response = client
            .count(elasticsearch::CountParts::None)
            .send()
            .await
            .int_err()?;

        #[derive(serde::Deserialize)]
        struct CountResponse {
            count: u64,
        }

        let body: CountResponse = response.json().await.int_err()?;
        let count = body.count;
        Ok(count)
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_documents_in_index, skip_all)]
    async fn documents_in_index(&self, kind: &str) -> Result<u64, InternalError> {
        let client = self
            .client
            .get_or_try_init(async || Self::es_client(&self.config))
            .await?;
        let full_index_name = self.full_index_name(kind);

        let response = client
            .count(elasticsearch::CountParts::Index(&[&full_index_name]))
            .send()
            .await
            .int_err()?;

        #[derive(serde::Deserialize)]
        struct CountResponse {
            count: u64,
        }

        let body: CountResponse = response.json().await.int_err()?;
        let count = body.count;
        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
