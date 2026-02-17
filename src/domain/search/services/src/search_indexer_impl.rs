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
use kamu_search::{
    SearchEntitySchema,
    SearchEntitySchemaProvider,
    SearchIndexer,
    SearchIndexerConfig,
    SearchRepository,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn SearchIndexer)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.search.SearchIndexer",
    depends_on: &[],
    requires_transaction: false,
})]
pub struct SearchIndexerImpl {
    indexer_config: Arc<SearchIndexerConfig>,
    search_repo: Arc<dyn SearchRepository>,
    entity_schema_providers: Vec<Arc<dyn SearchEntitySchemaProvider>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type ProviderSchemaPair = (Arc<dyn SearchEntitySchemaProvider>, SearchEntitySchema);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
impl SearchIndexerImpl {
    fn resolve_target_schemas(
        &self,
        entity_names: &[String],
    ) -> Result<Vec<ProviderSchemaPair>, InternalError> {
        let all_schemas = self
            .entity_schema_providers
            .iter()
            .flat_map(|provider| {
                provider
                    .provide_schemas()
                    .iter()
                    .map(|schema| (Arc::clone(provider), schema.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        if entity_names.is_empty() {
            return Ok(all_schemas);
        }

        let mut requested_unique = std::collections::BTreeSet::new();
        requested_unique.extend(entity_names.iter().map(String::as_str));

        let mut missing = Vec::new();
        let mut targets = Vec::new();
        for entity_name in requested_unique {
            let Some((provider, schema)) = all_schemas
                .iter()
                .find(|(_, schema)| schema.schema_name == entity_name)
            else {
                missing.push(entity_name);
                continue;
            };

            targets.push((Arc::clone(provider), schema.clone()));
        }

        if !missing.is_empty() {
            return Err(InternalError::new(format!(
                "Unknown search entity name(s): {}",
                missing.join(", ")
            )));
        }

        Ok(targets)
    }

    #[tracing::instrument(level = "info", name = SearchIndexerImpl_ensure_target_indexes_exist, skip_all)]
    async fn ensure_target_indexes_exist(
        &self,
        targets: &[ProviderSchemaPair],
    ) -> Result<(), InternalError> {
        for (_, schema) in targets {
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

        Ok(())
    }

    #[tracing::instrument(level = "info", name = SearchIndexerImpl_ensure_indexes_exist, skip_all)]
    // Public for tests only
    pub async fn ensure_indexes_exist(&self) -> Result<(), InternalError> {
        let targets = self.resolve_target_schemas(&[])?;

        for provider in &self.entity_schema_providers {
            tracing::info!(
                schemas_count = provider.provide_schemas().len(),
                provider_name = provider.provider_name(),
                "Registering search entity schemas from provider",
            );
        }

        self.ensure_target_indexes_exist(&targets).await
    }

    #[tracing::instrument(level = "info", name = SearchIndexerImpl_drop_all_schemas, skip_all)]
    async fn drop_all_schemas(&self) -> Result<(), InternalError> {
        self.search_repo.drop_all_schemas().await
    }

    #[tracing::instrument(level = "info", name = SearchIndexerImpl_run_indexing, skip_all)]
    async fn run_indexing(&self) -> Result<(), InternalError> {
        let targets = self.resolve_target_schemas(&[])?;
        self.run_target_indexing(&targets).await
    }

    #[tracing::instrument(level = "info", name = SearchIndexerImpl_run_target_indexing, skip_all)]
    async fn run_target_indexing(
        &self,
        targets: &[ProviderSchemaPair],
    ) -> Result<(), InternalError> {
        for (provider, schema) in targets {
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

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl SearchIndexer for SearchIndexerImpl {
    #[tracing::instrument(level = "info", name = SearchIndexerImpl_reset_search_indices, skip_all)]
    async fn reset_search_indices(&self, entity_names: Vec<String>) -> Result<(), InternalError> {
        let targets = self.resolve_target_schemas(&entity_names)?;

        for (provider, schema) in &targets {
            self.search_repo.lock_schema(schema.schema_name).await?;

            let reset_result = async {
                self.search_repo.drop_schemas(&[schema.schema_name]).await?;
                self.search_repo
                    .ensure_entity_index(schema)
                    .await
                    .map_err(ErrorIntoInternal::int_err)?;

                let num_indexed = provider
                    .run_schema_initial_indexing(self.search_repo.clone(), schema)
                    .await?;

                tracing::info!(
                    entity_kind = %schema.schema_name,
                    num_indexed,
                    "Completed full reindexing of search index for entity",
                );

                Ok::<(), InternalError>(())
            }
            .await;

            let unlock_result = self.search_repo.unlock_schema(schema.schema_name).await;
            unlock_result?;

            reset_result?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl InitOnStartup for SearchIndexerImpl {
    #[tracing::instrument(level = "info", name = SearchIndexerImpl_run_initialization, skip_all)]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        if self.indexer_config.clear_on_start {
            self.drop_all_schemas().await?;
        }

        self.ensure_indexes_exist().await?;
        self.run_indexing().await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
