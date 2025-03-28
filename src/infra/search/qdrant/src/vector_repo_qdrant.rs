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
use kamu_search::{FoundPoint, InsertError, NewPoint, SearchPointsOpts, VectorRepository};
use rand::RngCore;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct VectorRepositoryConfigQdrant {
    pub url: String,
    pub collection_name: String,
    pub dimensions: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct VectorRepositoryQdrant {
    config: Arc<VectorRepositoryConfigQdrant>,
    client: tokio::sync::OnceCell<qdrant_client::Qdrant>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn VectorRepository)]
impl VectorRepositoryQdrant {
    const UPSERT_POINTS_PER_CHUNK: usize = 1000;

    pub fn new(config: Arc<VectorRepositoryConfigQdrant>) -> Self {
        Self {
            config,
            client: tokio::sync::OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<&qdrant_client::Qdrant, InternalError> {
        self.client
            .get_or_try_init(async || self.init_client().await)
            .await
    }

    async fn init_client(&self) -> Result<qdrant_client::Qdrant, InternalError> {
        use ::qdrant_client::qdrant::*;

        let client = ::qdrant_client::Qdrant::from_url(&self.config.url)
            .build()
            .int_err()?;

        // Create collection if it does not exist
        if client
            .collection_exists(&self.config.collection_name)
            .await
            .int_err()?
        {
            return Ok(client);
        }

        // TODO: Make distance configurable?
        client
            .create_collection(
                CreateCollectionBuilder::new(&self.config.collection_name).vectors_config(
                    VectorParamsBuilder::new(self.config.dimensions as u64, Distance::Cosine),
                ),
            )
            .await
            .int_err()?;

        Ok(client)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl VectorRepository for VectorRepositoryQdrant {
    #[tracing::instrument(level = "info", skip_all)]
    async fn num_points(&self) -> Result<usize, InternalError> {
        let res = self
            .client()
            .await?
            .collection_info(&self.config.collection_name)
            .await
            .int_err()?;

        let points_count = res
            .result
            .expect("collection info result should not be empty")
            .points_count();

        Ok(usize::try_from(points_count).unwrap())
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn insert(&self, points: Vec<NewPoint>) -> Result<(), InsertError> {
        if points.is_empty() {
            return Ok(());
        }

        let points: Vec<_> = {
            let mut rng = rand::thread_rng();

            points
                .into_iter()
                .map(|point| {
                    qdrant_client::qdrant::PointStruct::new(
                        rng.next_u64(),
                        point.vector,
                        qdrant_client::Payload::try_from(point.payload).unwrap(),
                    )
                })
                .collect()
        };

        self.client()
            .await?
            .upsert_points_chunked(
                qdrant_client::qdrant::UpsertPointsBuilder::new(
                    &self.config.collection_name,
                    points,
                ),
                Self::UPSERT_POINTS_PER_CHUNK,
            )
            .await
            .int_err()?;

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all, fields(?opts))]
    async fn search_points(
        &self,
        vec: &[f32],
        opts: SearchPointsOpts,
    ) -> Result<Vec<FoundPoint>, InternalError> {
        use ::qdrant_client::qdrant::*;

        let res = self
            .client()
            .await?
            .search_points(
                SearchPointsBuilder::new(&self.config.collection_name, vec, opts.limit as u64)
                    .with_payload(true)
                    .offset(opts.skip as u64),
            )
            .await
            .int_err()?;

        Ok(res
            .result
            .into_iter()
            .map(|p| FoundPoint {
                point_id: match p
                    .id
                    .expect("point id should not be empty")
                    .point_id_options
                    .expect("point id options should not be empty")
                {
                    point_id::PointIdOptions::Num(v) => v.to_string(),
                    point_id::PointIdOptions::Uuid(v) => v.to_string(),
                },
                payload: serde_json::Value::Object(
                    p.payload
                        .into_iter()
                        .map(|(k, v)| (k, v.into_json()))
                        .collect(),
                ),
                score: p.score,
            })
            .collect())
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn clear(&self) -> Result<(), InternalError> {
        use ::qdrant_client::qdrant::*;

        self.client()
            .await?
            .delete_points(
                DeletePointsBuilder::new(&self.config.collection_name)
                    .points(Filter::default())
                    .wait(true)
                    .build(),
            )
            .await
            .int_err()?;

        Ok(())
    }
}
