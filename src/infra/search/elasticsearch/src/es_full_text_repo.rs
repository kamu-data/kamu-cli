// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_search::*;

use crate::es_helpers::ElasticSearchHighlightExtractor;
use crate::{ElasticSearchFullTextSearchConfig, es_client, es_helpers};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticSearchFullTextRepo {
    config: Arc<ElasticSearchFullTextSearchConfig>,
    client: tokio::sync::OnceCell<es_client::ElasticSearchClient>,
    state: std::sync::RwLock<State>,
}

#[derive(Default)]
struct State {
    registered_schemas: HashMap<FullTextEntitySchemaName, Arc<FullTextSearchEntitySchema>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn FullTextSearchRepository)]
impl ElasticSearchFullTextRepo {
    pub fn new(config: Arc<ElasticSearchFullTextSearchConfig>) -> Self {
        Self {
            config,
            client: tokio::sync::OnceCell::new(),
            state: std::sync::RwLock::new(State::default()),
        }
    }

    async fn es_client(&self) -> Result<&es_client::ElasticSearchClient, InternalError> {
        let client = self
            .client
            .get_or_try_init(async || es_client::ElasticSearchClient::init((*self.config).clone()))
            .await
            .int_err()?;
        Ok(client)
    }

    fn resolve_read_index_alias(
        &self,
        client: &es_client::ElasticSearchClient,
        schema_name: FullTextEntitySchemaName,
    ) -> Result<String, InternalError> {
        let state = self.state.read().unwrap();
        let Some(schema) = state.registered_schemas.get(&schema_name) else {
            return Err(InternalError::new(format!(
                "Entity schema '{schema_name}' is not registered in full-text search repository",
            )));
        };

        let entity_index = es_helpers::ElasticSearchVersionedEntityIndex::new(
            client,
            &self.config,
            schema.schema_name,
            schema.version,
        );
        Ok(entity_index.alias_name())
    }

    fn resolve_writable_index_name(
        &self,
        client: &es_client::ElasticSearchClient,
        schema_name: FullTextEntitySchemaName,
    ) -> Result<String, InternalError> {
        let state = self.state.read().unwrap();
        let Some(schema) = state.registered_schemas.get(&schema_name) else {
            return Err(InternalError::new(format!(
                "Entity schema '{schema_name}' is not registered in full-text search repository",
            )));
        };

        let entity_index = es_helpers::ElasticSearchVersionedEntityIndex::new(
            client,
            &self.config,
            schema.schema_name,
            schema.version,
        );
        Ok(entity_index.index_name())
    }

    fn resolve_entity_schemas(
        &self,
        schema_names: &[FullTextEntitySchemaName],
    ) -> Result<Vec<Arc<FullTextSearchEntitySchema>>, InternalError> {
        let state = self.state.read().unwrap();
        let mut schemas = Vec::new();
        if schema_names.is_empty() {
            // If no schema_names specified, use all registered schemas
            for schema in state.registered_schemas.values() {
                schemas.push(Arc::clone(schema));
            }
        } else {
            for schema_name in schema_names {
                let Some(schema) = state.registered_schemas.get(schema_name) else {
                    return Err(InternalError::new(format!(
                        "Entity schema '{schema_name}' is not registered in full-text search \
                         repository",
                    )));
                };
                schemas.push(Arc::clone(schema));
            }
        }
        Ok(schemas)
    }

    fn resolve_read_index_aliases(
        &self,
        client: &es_client::ElasticSearchClient,
        entity_schemas: &[Arc<FullTextSearchEntitySchema>],
    ) -> Result<HashMap<String, FullTextEntitySchemaName>, InternalError> {
        assert!(!entity_schemas.is_empty());

        entity_schemas
            .iter()
            .map(|schema| {
                let index_name = self.resolve_read_index_alias(client, schema.schema_name)?;
                Ok((index_name, schema.schema_name))
            })
            .collect::<Result<HashMap<_, _>, InternalError>>()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl FullTextSearchRepository for ElasticSearchFullTextRepo {
    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_health skip_all)]
    async fn health(&self) -> Result<serde_json::Value, InternalError> {
        let client = self.es_client().await?;
        client.cluster_health().await.int_err()
    }

    #[tracing::instrument(
        level = "debug",
        name=ElasticSearchFullTextRepo_ensure_entity_index,
        skip_all, fields(
            entity_kind = %schema.schema_name,
            version = schema.version
        )
    )]
    async fn ensure_entity_index(
        &self,
        schema: &FullTextSearchEntitySchema,
    ) -> Result<(), FullTextSearchEnsureEntityIndexError> {
        {
            // Check if schema is already registered
            let state = self.state.read().unwrap();
            if let Some(registered_schema) = state.registered_schemas.get(schema.schema_name) {
                tracing::info!(
                    entity_kind = %schema.schema_name,
                    "Full-text search entity schema is already registered, skipping",
                );
                assert!(registered_schema.version == schema.version);
                return Ok(());
            }
        }

        let index = es_helpers::ElasticSearchVersionedEntityIndex::new(
            self.es_client().await?,
            &self.config,
            schema.schema_name,
            schema.version,
        );

        let mappings = es_helpers::ElasticSearchIndexMappings::from_entity_schema(schema);

        let outcome = index.ensure_version_existence(mappings, schema).await?;

        use es_helpers::EntityIndexEnsureOutcome;
        match outcome {
            EntityIndexEnsureOutcome::UpToDate { .. }
            | EntityIndexEnsureOutcome::CreatedNew { .. }
            | EntityIndexEnsureOutcome::UpgradePerformed { .. } => {}
            EntityIndexEnsureOutcome::DriftDetected {
                alias,
                index,
                existing_version,
                expected_hash,
                actual_hash,
            } => {
                return Err(FullTextSearchEnsureEntityIndexError::SchemaDriftDetected {
                    schema_name: schema.schema_name,
                    version: existing_version,
                    alias,
                    index,
                    expected_hash,
                    actual_hash,
                });
            }
            EntityIndexEnsureOutcome::DowngradeAttempted {
                alias,
                index,
                existing_version,
                attempted_version,
            } => {
                return Err(FullTextSearchEnsureEntityIndexError::DowngradeAttempted {
                    schema_name: schema.schema_name,
                    existing_version,
                    attempted_version,
                    alias,
                    index,
                });
            }
        }

        {
            // Register schema as registered
            let mut state = self.state.write().unwrap();
            state
                .registered_schemas
                .insert(schema.schema_name, Arc::new(schema.clone()));
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_total_documents, skip_all)]
    async fn total_documents(&self) -> Result<u64, InternalError> {
        let client = self.es_client().await?;
        client.total_documents().await.int_err()
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_documents_of_kind, skip_all, fields(schema_name))]
    async fn documents_of_kind(
        &self,
        schema_name: FullTextEntitySchemaName,
    ) -> Result<u64, InternalError> {
        let client = self.es_client().await?;
        let index_name = self.resolve_writable_index_name(client, schema_name)?; // wriatable index, because we need to count bannned too
        client.documents_in_index(&index_name).await.int_err()
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_search, skip_all)]
    async fn search(
        &self,
        req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError> {
        let client = self.es_client().await?;

        // Resolve entity schemas
        let entity_schemas = self.resolve_entity_schemas(&req.entity_schemas)?;

        // Resolve affected index names
        let schema_names_by_index_names =
            self.resolve_read_index_aliases(client, &entity_schemas)?;

        // Involved indexes
        let involved_index_names: Vec<&str> = schema_names_by_index_names
            .keys()
            .map(std::string::String::as_str)
            .collect();

        // Build ElasticSearch request body
        let req_body = es_helpers::ElasticSearchQueryBuilder::build_search_query(&req);

        // Execute request
        let es_response: es_client::SearchResponse = client
            .search(req_body, &involved_index_names)
            .await
            .int_err()?;

        // Translate parsed ElasticSearch response to domain model
        Ok(FullTextSearchResponse {
            took_ms: es_response.took,
            timeout: es_response.timed_out,
            total_hits: es_response.hits.total.value,
            hits: es_response
                .hits
                .hits
                .into_iter()
                .map(|hit| FullTextSearchHit {
                    id: hit.id.unwrap_or_default(),
                    schema_name: schema_names_by_index_names
                        .get(&hit.index)
                        .copied()
                        .unwrap_or("<unknown>"),
                    score: hit.score,
                    source: hit.source.unwrap_or_default(),
                    highlights: if let Some(highlight_json) = hit.highlight {
                        ElasticSearchHighlightExtractor::extract_highlights(&highlight_json)
                    } else {
                        None
                    },
                    explanation: hit.explanation,
                })
                .collect(),
        })
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_find_document_by_id, skip_all, fields(schema_name, id))]
    async fn find_document_by_id(
        &self,
        schema_name: FullTextEntitySchemaName,
        id: &FullTextEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError> {
        let client = self.es_client().await?;
        let index_name = self.resolve_read_index_alias(client, schema_name)?;
        let doc = client
            .find_document_by_id(&index_name, id)
            .await
            .int_err()?;
        Ok(doc)
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_bulk_update, skip_all, fields(schema_name, num_operations = operations.len()))]
    async fn bulk_update(
        &self,
        schema_name: FullTextEntitySchemaName,
        operations: Vec<FullTextUpdateOperation>,
    ) -> Result<(), InternalError> {
        let client = self.es_client().await?;
        let index_name = self.resolve_writable_index_name(client, schema_name)?;
        client.bulk_update(&index_name, operations).await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
