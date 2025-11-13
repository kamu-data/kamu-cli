// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::InternalError;
use kamu_datasets::JOB_KAMU_DATASETS_DATASET_BLOCK_INDEXER;
use kamu_search::{FullTextSearchEntitySchemaProvider, FullTextSearchRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.search.FullTextSearchIndexer",
    depends_on: &[JOB_KAMU_DATASETS_DATASET_BLOCK_INDEXER],
    requires_transaction: true,
})]
pub struct FullTextSearchIndexer {
    full_text_repo: Arc<dyn FullTextSearchRepository>,
    entity_schema_providers: Vec<Arc<dyn FullTextSearchEntitySchemaProvider>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
impl FullTextSearchIndexer {
    #[tracing::instrument(level = "info", name = FullTextSearchIndexer_ensure_indexes_exist, skip_all)]
    async fn ensure_indexes_exist(&self) -> Result<(), InternalError> {
        // Request schemas from all providers and ensure indexes exist
        for provider in &self.entity_schema_providers {
            let schemas = provider.provide_schemas();
            tracing::info!(
                "Registering {} full-text search entity schemas from provider '{}'",
                schemas.len(),
                provider.provider_name()
            );

            // Ensure indexes exist for each schema
            for schema in schemas {
                // TODO: handle schema versioning/migrations

                if !self
                    .full_text_repo
                    .has_entity_index(schema.entity_kind)
                    .await?
                {
                    tracing::info!(
                        "Creating full-text search index for entity kind '{}'",
                        schema.entity_kind
                    );
                    self.full_text_repo.create_entity_index(schema).await?;
                } else {
                    tracing::info!(
                        "Full-text search index for entity kind '{}' already exists",
                        schema.entity_kind
                    );
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl InitOnStartup for FullTextSearchIndexer {
    #[tracing::instrument(level = "info", name = FullTextSearchIndexer_run_initialization, skip_all)]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        self.ensure_indexes_exist().await?;

        // TODO: documents indexing logic

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
