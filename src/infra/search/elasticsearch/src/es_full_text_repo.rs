// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use elasticsearch::Elasticsearch;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_search::*;

use crate::ElasticSearchFullTextSearchConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DEFAULT_ELASTICSEARCH_USER: &str = "elastic";
const DEFAULT_ELASTICSEARCH_PASSWORD: &str = "root";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticSearchFullTextRepo {
    config: Arc<ElasticSearchFullTextSearchConfig>,
    client: tokio::sync::OnceCell<Elasticsearch>,
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
    ) -> Result<Elasticsearch, InternalError> {
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
        Ok(Elasticsearch::new(transport))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FullTextSearchRepository for ElasticSearchFullTextRepo {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
