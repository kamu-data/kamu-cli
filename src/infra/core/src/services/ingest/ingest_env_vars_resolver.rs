// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_core::IngestEnvVars;
use kamu_datasets::{DatasetEnvVarResolver, SecretsEncryptionConfig};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves a dataset's environment variables for the fetch step of a polling
/// ingest. They are enabled only when secrets encryption is configured and
/// enabled and a resolver is registered; otherwise every dataset has none.
/// The values are never part of a plan: resolve them for each execution.
pub struct IngestEnvVarsResolver {
    maybe_resolver: Option<Arc<dyn DatasetEnvVarResolver>>,
}

#[common_macros::method_names_consts]
#[dill::component(pub)]
impl IngestEnvVarsResolver {
    pub fn new(
        maybe_secrets_encryption_config: Option<Arc<SecretsEncryptionConfig>>,
        maybe_resolver: Option<Arc<dyn DatasetEnvVarResolver>>,
    ) -> Self {
        let enabled = maybe_secrets_encryption_config.is_some_and(|config| config.is_enabled());
        Self {
            maybe_resolver: maybe_resolver.filter(|_| enabled),
        }
    }

    #[tracing::instrument(name = IngestEnvVarsResolver_resolve, level = "debug", skip_all, fields(%dataset_id))]
    pub async fn resolve(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<IngestEnvVars, InternalError> {
        let Some(resolver) = &self.maybe_resolver else {
            return Ok(IngestEnvVars::default());
        };

        let env_vars = resolver.resolve_effective_env_vars(dataset_id).await?;
        Ok(IngestEnvVars::new(env_vars))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
