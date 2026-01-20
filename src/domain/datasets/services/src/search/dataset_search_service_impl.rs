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

use kamu_datasets::*;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DatasetSearchService)]
pub struct DatasetSearchServiceImpl {
    search_service: Arc<dyn SearchService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetSearchService for DatasetSearchServiceImpl {
    async fn vector_search(
        &self,
        ctx: SearchContext<'_>,
        prompt: String,
        pagination: SearchPaginationSpec,
    ) -> Result<DatasetSearchResponse, DatasetSearchError> {
        // Run vector search request
        let search_results = {
            use kamu_datasets::dataset_search_schema as dataset_schema;
            use kamu_search::*;

            self.search_service
                .vector_search(
                    ctx,
                    SearchRequest {
                        query: if prompt.is_empty() {
                            None
                        } else {
                            Some(prompt)
                        },
                        source: SearchRequestSourceSpec::None,
                        entity_schemas: vec![dataset_schema::SCHEMA_NAME],
                        filter: None,
                        sort: vec![],
                        page: pagination,
                        options: SearchOptions {
                            enable_explain: false,
                            enable_highlighting: false,
                        },
                    },
                )
                .await
        }?;

        // Extract dataset IDs and resolve handles
        let dataset_ids: Vec<_> = search_results
            .hits
            .iter()
            .map(|hit| Cow::Owned(odf::DatasetID::from_did_str(&hit.id).unwrap()))
            .collect();

        let dataset_handles_resolution = self
            .dataset_registry
            .resolve_multiple_dataset_handles_by_ids(&dataset_ids)
            .await
            .map_err(|e| match e {
                kamu_datasets::GetMultipleDatasetsError::Internal(e) => e,
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
            total_hits: search_results.total_hits,
            hits: search_results
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
