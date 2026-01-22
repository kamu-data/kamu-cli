// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_datasets::*;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DatasetSearchService)]
pub struct DatasetSearchServiceImpl {
    search_service: Arc<dyn SearchService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    embeddings_encoder: Arc<dyn EmbeddingsEncoder>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetSearchServiceImpl {
    async fn convert_search_response(
        &self,
        search_response: SearchResponse,
    ) -> Result<DatasetSearchResponse, DatasetSearchError> {
        // Extract dataset IDs and resolve handles
        let dataset_ids: Vec<_> = search_response
            .hits
            .iter()
            .map(|hit| Cow::Owned(odf::DatasetID::from_did_str(&hit.id).unwrap()))
            .collect();

        let dataset_handles_resolution = self
            .dataset_registry
            .resolve_multiple_dataset_handles_by_ids(&dataset_ids)
            .await
            .map_err(|e| match e {
                kamu_datasets::GetMultipleDatasetsError::Internal(e) => {
                    DatasetSearchError::Internal(e)
                }
            })?;

        // Build id -> handle mapping
        let resolved_datasets_by_ids = dataset_handles_resolution
            .resolved_handles
            .into_iter()
            .map(|hdl| (hdl.id.clone(), hdl))
            .collect::<HashMap<_, _>>();

        // Warn about unresolved datasets - this is weird,
        // but it might happen in case of eventual consistency
        for (unresolved_dataset_id, err) in dataset_handles_resolution.unresolved_datasets {
            tracing::warn!(
              dataset_id = %unresolved_dataset_id,
              error = ?err,
              error_msg = %err,
              "DatasetSearchServiceImpl: Could not resolve dataset handle returned by search index",
            );
        }

        // Note: we assume that ReBAC is handled by the search layer itself

        // Build response
        Ok(DatasetSearchResponse {
            total_hits: search_response.total_hits,
            hits: search_response
                .hits
                .into_iter()
                .filter_map(|hit| {
                    let dataset_id = odf::DatasetID::from_did_str(&hit.id).ok()?;
                    let handle = resolved_datasets_by_ids.get(&dataset_id)?.clone();
                    Some(DatasetSearchHit {
                        handle,
                        score: hit.score.unwrap_or_default(),
                    })
                })
                .collect(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetSearchService for DatasetSearchServiceImpl {
    async fn vector_search(
        &self,
        ctx: SearchContext<'_>,
        prompt: String,
        limit: usize,
    ) -> Result<DatasetSearchResponse, DatasetSearchError> {
        // Protect against empty prompts
        let prompt = prompt.trim();
        if prompt.is_empty() {
            return Ok(DatasetSearchResponse {
                total_hits: None,
                hits: vec![],
            });
        }

        // Build embeddings for the prompt
        let prompt_vec = self
            .embeddings_encoder
            .encode(vec![prompt.to_string()])
            .await
            .int_err()?
            .into_iter()
            .next()
            .unwrap();

        // Run vector search request
        let search_response = {
            use kamu_datasets::dataset_search_schema as dataset_schema;
            use kamu_search::*;

            self.search_service
                .vector_search(
                    ctx,
                    VectorSearchRequest {
                        prompt_embedding: prompt_vec,
                        source: SearchRequestSourceSpec::None,
                        entity_schemas: vec![dataset_schema::SCHEMA_NAME],
                        filter: None,
                        limit,
                        options: VectorSearchOptions {
                            enable_explain: false,
                            knn: KnnOptions::for_limit(limit),
                        },
                    },
                )
                .await
        }?;

        // Convert results into dataset handles
        self.convert_search_response(search_response).await
    }

    async fn hybrid_search(
        &self,
        ctx: SearchContext<'_>,
        prompt: String,
        limit: usize,
    ) -> Result<DatasetSearchResponse, DatasetSearchError> {
        // Protect against empty prompts
        let prompt = prompt.trim();
        if prompt.is_empty() {
            return Ok(DatasetSearchResponse {
                total_hits: None,
                hits: vec![],
            });
        }

        // Build embeddings for the prompt
        let prompt_embedding = self
            .embeddings_encoder
            .encode(vec![prompt.to_string()])
            .await
            .int_err()?
            .into_iter()
            .next()
            .unwrap();

        // Run vector search request
        let search_response = {
            use kamu_datasets::dataset_search_schema as dataset_schema;
            use kamu_search::*;

            self.search_service
                .hybrid_search(
                    ctx,
                    HybridSearchRequest {
                        prompt: prompt.to_string(),
                        prompt_embedding,
                        source: SearchRequestSourceSpec::None,
                        entity_schemas: vec![dataset_schema::SCHEMA_NAME],
                        filter: None,
                        limit,
                        options: HybridSearchOptions::default(),
                    },
                )
                .await
        }?;

        // Convert results into dataset handles
        self.convert_search_response(search_response).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
