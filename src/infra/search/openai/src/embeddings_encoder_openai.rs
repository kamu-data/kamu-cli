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
use kamu_search::EmbeddingsEncoder;
use secrecy::ExposeSecret as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EmbeddingsEncoderConfigOpenAI {
    pub url: Option<String>,

    pub api_key: Option<secrecy::SecretString>,

    pub model_name: String,

    pub dimensions: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EmbeddingsEncoderOpenAi {
    config: Arc<EmbeddingsEncoderConfigOpenAI>,
}

#[dill::component(pub)]
#[dill::interface(dyn EmbeddingsEncoder)]
impl EmbeddingsEncoderOpenAi {
    pub fn new(config: Arc<EmbeddingsEncoderConfigOpenAI>) -> Self {
        Self { config }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EmbeddingsEncoder for EmbeddingsEncoderOpenAi {
    #[tracing::instrument(level = "info", skip_all)]
    async fn encode(&self, input: Vec<String>) -> Result<Vec<Vec<f32>>, InternalError> {
        let mut config = async_openai::config::OpenAIConfig::default();
        if let Some(url) = &self.config.url {
            config = config.with_api_base(url);
        }
        if let Some(api_key) = &self.config.api_key {
            config = config.with_api_key(api_key.expose_secret());
        }

        let client = async_openai::Client::with_config(config);

        // TODO: Handle too many tokens?
        let embedding_request = async_openai::types::CreateEmbeddingRequestArgs::default()
            .model(&self.config.model_name)
            .input(input)
            .build()
            .int_err()?;

        let response = client
            .embeddings()
            .create(embedding_request)
            .await
            .int_err()?;

        Ok(response.data.into_iter().map(|e| e.embedding).collect())
    }
}
