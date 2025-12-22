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
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_datasets::JOB_KAMU_DATASETS_DATASET_BLOCK_INDEXER;
use kamu_search::{SearchEntitySchemaProvider, SearchRepository};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.search.SearchIndexer",
    depends_on: &[JOB_KAMU_DATASETS_DATASET_BLOCK_INDEXER],
    requires_transaction: true,
})]
pub struct SearchIndexer {
    search_repo: Arc<dyn SearchRepository>,
    entity_schema_providers: Vec<Arc<dyn SearchEntitySchemaProvider>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
impl SearchIndexer {
    #[tracing::instrument(level = "info", name = SearchIndexer_ensure_indexes_exist, skip_all)]
    async fn ensure_indexes_exist(&self) -> Result<(), InternalError> {
        // Request schemas from all providers and ensure indexes exist
        for provider in &self.entity_schema_providers {
            let schemas = provider.provide_schemas();
            tracing::info!(
                schemas_count = schemas.len(),
                provider_name = provider.provider_name(),
                "Registering search entity schemas from provider",
            );

            // Ensure indexes exist for each schema
            for schema in schemas {
                match self.search_repo.ensure_entity_index(schema).await {
                    Ok(outcome) => {
                        tracing::info!(
                            entity_kind = %schema.schema_name,
                            version = schema.version,
                            outcome = ?outcome,
                            "Ensured up-to-date search index for entity",
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            entity_kind = %schema.schema_name,
                            error = ?e,
                            error_msg = %e,
                            "Failed to ensure search index for entity",
                        );
                        return Err(e.int_err());
                    }
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", name = SearchIndexer_run_indexing, skip_all)]
    async fn run_indexing(&self) -> Result<(), InternalError> {
        for provider in &self.entity_schema_providers {
            let schemas = provider.provide_schemas();
            for schema in schemas {
                let num_existing_documents = self
                    .search_repo
                    .documents_of_kind(schema.schema_name)
                    .await?;
                if num_existing_documents == 0 {
                    tracing::info!(
                        entity_kind = %schema.schema_name,
                        "No existing documents found, running full reindexing",
                    );
                    match provider
                        .run_schema_initial_indexing(self.search_repo.clone(), schema)
                        .await
                    {
                        Ok(num_indexed) => {
                            tracing::info!(
                                entity_kind = %schema.schema_name,
                                num_indexed,
                                "Completed full reindexing of search index for entity",
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                entity_kind = %schema.schema_name,
                                error = ?e,
                                error_msg = %e,
                                "Failed to run full reindexing of search index for entity",
                            );
                            return Err(e);
                        }
                    }
                } else {
                    tracing::info!(
                        entity_kind = %schema.schema_name,
                        num_existing_documents,
                        "Existing documents found, skipping full reindexing",
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
impl InitOnStartup for SearchIndexer {
    #[tracing::instrument(level = "info", name = SearchIndexer_run_initialization, skip_all)]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        self.ensure_indexes_exist().await?;
        self.run_indexing().await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
