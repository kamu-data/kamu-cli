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
    pub const DEFAULT_POINTS_LIMIT: usize = 10;

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
        let prompt_vec = self
            .embeddings_encoder
            .encode(vec![prompt.to_string()])
            .await
            .int_err()?
            .into_iter()
            .next()
            .unwrap();

        let points = self
            .vector_repo
            .search_points(
                prompt_vec,
                SearchPointsOpts {
                    limit: options.limit.unwrap_or(Self::DEFAULT_POINTS_LIMIT),
                },
            )
            .await
            .int_err()?;

        let mut datasets = Vec::new();
        for p in points {
            let dataset_id = p.payload["id"].as_str().ok_or_else(|| {
                InternalError::new(format!(
                    "Point {} does not have associated dataset ID",
                    p.point_id
                ))
            })?;

            let dataset_id = odf::DatasetID::from_did_str(dataset_id).int_err()?;

            // TODO: Vectorize
            let Some(handle) = self
                .dataset_registry
                .try_resolve_dataset_handle_by_ref(&dataset_id.as_local_ref())
                .await?
            else {
                return Err(InternalError::new(format!(
                    "Point {} refers to dataset ID {} which is not present in the registry",
                    p.point_id, dataset_id,
                ))
                .into());
            };

            datasets.push(SearchLocalResultDataset {
                handle,
                score: p.score,
            });
        }

        Ok(SearchLocalNatLangResult { datasets })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
