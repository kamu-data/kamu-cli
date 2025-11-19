// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_search::*;

use crate::{ElasticSearchFullTextSearchConfig, es_client, es_helpers};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticSearchFullTextRepo {
    config: Arc<ElasticSearchFullTextSearchConfig>,
    client: tokio::sync::OnceCell<es_client::ElasticSearchClient>,
    state: std::sync::RwLock<State>,
}

#[derive(Default)]
struct State {
    registered_schemas: HashMap<FullTextEntityKind, FullTextSearchEntitySchema>,
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

    fn resolve_index_name(
        &self,
        client: &es_client::ElasticSearchClient,
        kind: FullTextEntityKind,
    ) -> Result<String, InternalError> {
        let state = self.state.read().unwrap();
        let Some(schema) = state.registered_schemas.get(&kind) else {
            return Err(InternalError::new(format!(
                "Entity kind '{kind}' is not registered in full-text search repository",
            )));
        };

        let entity_index = es_helpers::ElasticSearchVersionedEntityIndex::new(
            client,
            &self.config,
            schema.entity_kind,
            schema.version,
        );
        Ok(entity_index.index_name())
    }

    fn resolve_entity_kinds_to_index_names(
        &self,
        client: &es_client::ElasticSearchClient,
        kinds: &[FullTextEntityKind],
    ) -> Result<HashMap<String, FullTextEntityKind>, InternalError> {
        // If no kinds specified, use all registered schemas
        let kinds_to_resolve: Vec<FullTextEntityKind> = if kinds.is_empty() {
            let state = self.state.read().unwrap();
            state.registered_schemas.keys().copied().collect()
        } else {
            kinds.to_vec()
        };

        kinds_to_resolve
            .iter()
            .map(|kind| {
                let index_name = self.resolve_index_name(client, kind)?;
                Ok((index_name, *kind))
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
            entity_kind = %entity_schema.entity_kind,
            version = entity_schema.version
        )
    )]
    async fn ensure_entity_index(
        &self,
        entity_schema: &FullTextSearchEntitySchema,
    ) -> Result<(), FullTextSearchEnsureEntityIndexError> {
        {
            // Check if schema is already registered
            let state = self.state.read().unwrap();
            if let Some(registered_schema) = state.registered_schemas.get(entity_schema.entity_kind)
            {
                tracing::info!(
                    entity_kind = %entity_schema.entity_kind,
                    "Full-text search entity schema is already registered, skipping",
                );
                assert!(registered_schema.version == entity_schema.version);
                return Ok(());
            }
        }

        let index = es_helpers::ElasticSearchVersionedEntityIndex::new(
            self.es_client().await?,
            &self.config,
            entity_schema.entity_kind,
            entity_schema.version,
        );

        let mappings = es_helpers::ElasticSearchIndexMappings::from_entity_schema(entity_schema);

        let outcome = index
            .ensure_version_existence(mappings, entity_schema.upgrade_mode)
            .await?;

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
                    entity_kind: entity_schema.entity_kind,
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
                    entity_kind: entity_schema.entity_kind,
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
                .insert(entity_schema.entity_kind, entity_schema.clone());
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_total_documents, skip_all)]
    async fn total_documents(&self) -> Result<u64, InternalError> {
        let client = self.es_client().await?;
        client.total_documents().await.int_err()
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_documents_of_kind, skip_all, fields(kind))]
    async fn documents_of_kind(&self, kind: FullTextEntityKind) -> Result<u64, InternalError> {
        let client = self.es_client().await?;
        let index_name = self.resolve_index_name(client, kind)?;
        client.documents_in_index(&index_name).await.int_err()
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_search, skip_all)]
    async fn search(
        &self,
        req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError> {
        let client = self.es_client().await?;

        // Resolve affected index names
        let entity_kinds_by_index_names =
            self.resolve_entity_kinds_to_index_names(client, &req.kinds)?;

        // Involved indexes
        let involved_index_names: Vec<&str> = entity_kinds_by_index_names
            .keys()
            .map(std::string::String::as_str)
            .collect();

        // Build ElasticSearch request body
        let req_body = es_helpers::ElasticSearchQueryBuilder::build_search_query(&req);
        // println!("\nES Search request: {req_body:?}, indexes:
        // {involved_index_names:?}\n",);

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
                    kind: entity_kinds_by_index_names
                        .get(&hit.index)
                        .copied()
                        .unwrap_or("<unknown>"),
                    score: hit.score,
                    source: hit.source.unwrap_or_default(),
                    highlights: BTreeMap::new(), // TODO: extract from highlight
                })
                .collect(),
        })
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_index_bulk, skip_all, fields(kind, num_docs = docs.len()))]
    async fn index_bulk(
        &self,
        kind: FullTextEntityKind,
        docs: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        let client = self.es_client().await?;
        let index_name = self.resolve_index_name(client, kind)?;
        client.bulk_index(&index_name, docs).await.int_err()
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_update_bulk, skip_all, fields(kind, num_updates = updates.len()))]
    async fn update_bulk(
        &self,
        kind: FullTextEntityKind,
        updates: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        let client = self.es_client().await?;
        let index_name = self.resolve_index_name(client, kind)?;
        client.bulk_update(&index_name, updates).await.int_err()
    }

    #[tracing::instrument(level = "debug", name=ElasticSearchFullTextRepo_delete_bulk, skip_all, fields(kind, num_ids = ids.len()))]
    async fn delete_bulk(
        &self,
        kind: FullTextEntityKind,
        ids: Vec<FullTextEntityId>,
    ) -> Result<(), InternalError> {
        let client = self.es_client().await?;
        let index_name = self.resolve_index_name(client, kind)?;
        client.bulk_delete(&index_name, ids).await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
