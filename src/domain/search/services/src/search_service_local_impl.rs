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
use itertools::Itertools;
use kamu_core::auth::DatasetActionAuthorizer;
use kamu_core::DatasetRegistry;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SearchServiceLocalImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    embeddings_encoder: Arc<dyn EmbeddingsEncoder>,
    vector_repo: Arc<dyn VectorRepository>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn SearchServiceLocal)]
impl SearchServiceLocalImpl {
    /// Vector store returns results containing all datasets including private,
    /// so we need to apply access policies to filter the inaccessible ones.
    /// As we filter, we are likely to end up with less results than was
    /// requested. To produce the desired number of results without querying too
    /// many times we request more points than was originally asked for.
    const OVERFETCH_FACTOR: usize = 2;

    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        embeddings_encoder: Arc<dyn EmbeddingsEncoder>,
        vector_repo: Arc<dyn VectorRepository>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_registry,
            embeddings_encoder,
            vector_repo,
            dataset_action_authorizer,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SearchServiceLocal for SearchServiceLocalImpl {
    async fn search_natural_language(
        &self,
        prompt: &str,
        options: SearchNatLangOpts,
    ) -> Result<SearchLocalNatLangResult, SearchLocalNatLangError> {
        // Encode prompt
        let prompt_vec = self
            .embeddings_encoder
            .encode(vec![prompt.to_string()])
            .await
            .int_err()?
            .into_iter()
            .next()
            .unwrap();

        // Search for nearby points
        let points = self
            .vector_repo
            .search_points(
                prompt_vec,
                SearchPointsOpts {
                    skip: 0,
                    limit: options.limit * Self::OVERFETCH_FACTOR,
                },
            )
            .await
            .int_err()?;

        // Extract dataset IDs from point payload
        let points_with_dataset_ids: Vec<(FoundPoint, odf::DatasetID)> = points
            .into_iter()
            .map(|p| match p.payload["id"].as_str() {
                Some(sid) => odf::DatasetID::from_did_str(sid)
                    .int_err()
                    .map(|id| (p, id)),
                None => Err(InternalError::new(format!(
                    "Point {} does not have associated dataset ID",
                    p.point_id
                ))),
            })
            .try_collect()?;

        // Resolve dataset handles
        let dataset_ids: Vec<odf::DatasetID> = points_with_dataset_ids
            .iter()
            .map(|(_pooint, id)| id.clone())
            .collect();

        let dataset_handles_res = self
            .dataset_registry
            .resolve_multiple_dataset_handles_by_ids(&dataset_ids)
            .await
            .int_err()?;

        let dataset_handles = dataset_handles_res.resolved_handles;

        // Not being able to resolve an ID can be simply due to eventual consistency or
        // can be a sign of a bug, so we log it
        for (id, _) in dataset_handles_res.unresolved_datasets {
            tracing::warn!(
                dataset_id = %id,
                "Ignoring point that refers to a dataset not present in the registry",
            );
        }

        // Filter by accessibility
        let dataset_handles = self
            .dataset_action_authorizer
            .filter_datasets_allowing(dataset_handles, kamu_core::auth::DatasetAction::Read)
            .await?;

        // Merge found points with accessible handles
        let dataset_handles_by_id: std::collections::BTreeMap<odf::DatasetID, odf::DatasetHandle> =
            dataset_handles
                .into_iter()
                .map(|h| (h.id.clone(), h))
                .collect();

        // Note: We trim per requested limit to compensate for overfetch
        let datasets: Vec<SearchLocalResultDataset> = points_with_dataset_ids
            .into_iter()
            .filter_map(|(p, id)| match dataset_handles_by_id.get(&id) {
                None => None,
                Some(handle) => Some(SearchLocalResultDataset {
                    handle: handle.clone(),
                    score: p.score,
                }),
            })
            .take(options.limit)
            .collect();

        Ok(SearchLocalNatLangResult { datasets })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
