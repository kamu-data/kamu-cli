// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Stores embedding vectors and allows to search for nearest points efficiently
#[async_trait::async_trait]
pub trait VectorRepository: Send + Sync {
    async fn num_points(&self) -> Result<usize, InternalError>;

    async fn upsert(&self, points: Vec<NewPoint>) -> Result<(), UpsertError>;

    async fn search_points(
        &self,
        vec: Vec<f32>,
        opts: SearchPointsOpts,
    ) -> Result<Vec<FoundPoint>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct SearchPointsOpts {
    pub limit: usize,
}

pub struct NewPoint {
    pub vector: Vec<f32>,
    pub payload: serde_json::Value,
}

pub struct FoundPoint {
    pub point_id: String,
    pub payload: serde_json::Value,
    pub score: f32,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum UpsertError {
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
