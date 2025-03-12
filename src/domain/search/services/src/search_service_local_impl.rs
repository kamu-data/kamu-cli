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
use kamu_core::{DatasetRegistry, DatasetRegistryExt as _};
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SearchServiceLocalImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    embeddings_encoder: Arc<dyn EmbeddingsEncoder>,
    vector_repo: Arc<dyn VectorRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn SearchServiceLocal)]
impl SearchServiceLocalImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        embeddings_encoder: Arc<dyn EmbeddingsEncoder>,
        vector_repo: Arc<dyn VectorRepository>,
    ) -> Self {
        Self {
            dataset_registry,
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
                    limit: options.limit,
                },
            )
            .await
            .int_err()?;

        // Map points to dataset IDs
        let dataset_ids: Vec<_> = points
            .iter()
            .map(|p| {
                p.payload["id"].as_str().ok_or_else(|| {
                    InternalError::new(format!(
                        "Point {} does not have associated dataset ID",
                        p.point_id
                    ))
                })
            })
            .map(|r| match r {
                Ok(sid) => odf::DatasetID::from_did_str(sid).int_err(),
                Err(e) => Err(e),
            })
            .try_collect()?;

        // Resolve dataset handles
        let dataset_handles = self
            .dataset_registry
            .try_resolve_multiple_dataset_handles_by_ids_stable(&dataset_ids)
            .await?;

        // Combine points and handles
        let datasets: Vec<_> = dataset_handles
            .into_iter()
            .zip(points)
            .filter_map(|(resolve, point)| match resolve {
                Ok(handle) => Some(SearchLocalResultDataset {
                    handle,
                    score: point.score,
                }),
                Err(err) => {
                    tracing::warn!(
                        point_id = point.point_id,
                        dataset_id = %err.dataset_ref,
                        "Ignoring point that refers to a dataset not present in the registry"
                    );
                    None
                }
            })
            .collect();

        Ok(SearchLocalNatLangResult { datasets })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
