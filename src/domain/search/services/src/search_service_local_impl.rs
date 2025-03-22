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
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    embeddings_encoder: Option<Arc<dyn EmbeddingsEncoder>>,
    vector_repo: Option<Arc<dyn VectorRepository>>,
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
    const OVERFETCH_AMOUNT: usize = 10;

    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        embeddings_encoder: Option<Arc<dyn EmbeddingsEncoder>>,
        vector_repo: Option<Arc<dyn VectorRepository>>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_action_authorizer,
            embeddings_encoder,
            vector_repo,
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
        let (Some(embeddings_encoder), Some(vector_repo)) = (
            self.embeddings_encoder.as_deref(),
            self.vector_repo.as_deref(),
        ) else {
            return Err(NatLangSearchNotEnabled.into());
        };

        // Encode prompt
        let prompt_vec = embeddings_encoder
            .encode(vec![prompt.to_string()])
            .await
            .int_err()?
            .into_iter()
            .next()
            .unwrap();

        // We will query vector store in a loop until desired number of results was
        // reached or search space was exhausted.
        let mut datasets: Vec<SearchLocalResultDataset> = Vec::new();
        let mut points_to_skip = 0;
        let points_limit = options.limit * Self::OVERFETCH_FACTOR + Self::OVERFETCH_AMOUNT;

        loop {
            // Search for nearby points
            let points = vector_repo
                .search_points(
                    &prompt_vec,
                    SearchPointsOpts {
                        skip: points_to_skip,
                        limit: points_limit,
                    },
                )
                .await
                .int_err()?;

            // Adjust skip in case we'll need to make another request
            points_to_skip += points_limit;
            let has_more_points = points.len() >= points_limit;

            // Extract dataset IDs from point payload
            let points_with_dataset_ids: Vec<(FoundPoint, odf::DatasetID)> = points
                .into_iter()
                .map(|p| match p.payload["dataset_id"].as_str() {
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
            let dataset_handles_res = self
                .dataset_registry
                .resolve_multiple_dataset_handles_by_ids(
                    points_with_dataset_ids
                        .iter()
                        .map(|(_pooint, id)| id.clone())
                        .collect(),
                )
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
            let dataset_handles_by_id: std::collections::BTreeMap<
                odf::DatasetID,
                odf::DatasetHandle,
            > = dataset_handles
                .into_iter()
                .map(|h| (h.id.clone(), h))
                .collect();

            // Note: We trim to compensate for overfetch
            datasets.extend(
                points_with_dataset_ids
                    .into_iter()
                    .filter_map(|(p, id)| match dataset_handles_by_id.get(&id) {
                        None => None,
                        Some(handle) => Some(SearchLocalResultDataset {
                            handle: handle.clone(),
                            score: p.score,
                        }),
                    })
                    .take(options.limit - datasets.len()),
            );

            // Break if we reached desired limit or exchausted the search
            if datasets.len() >= options.limit || !has_more_points {
                break;
            }
        }

        Ok(SearchLocalNatLangResult { datasets })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
