// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct VectorSearchRequest {
    /// Embedding vector to search for
    pub prompt_embedding: Vec<f32>,

    /// Allowed entity types (empty means all)
    pub entity_schemas: Vec<SearchEntitySchemaName>,

    /// Requested source fields. If empty, only IDs will be returned.
    pub source: SearchRequestSourceSpec,

    /// Structured filter
    pub filter: Option<SearchFilterExpr>,

    /// Max results to return
    pub limit: usize,

    /// Options
    pub options: VectorSearchOptions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default, Clone, Copy)]
pub struct VectorSearchOptions {
    pub knn: KnnOptions,
    pub enable_explain: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub struct KnnOptions {
    pub k: usize,
    pub num_candidates: usize,
}

impl KnnOptions {
    pub fn for_limit(limit: usize) -> Self {
        let k = (10 * limit).clamp(50, 200);
        let num_candidates = (10 * k).clamp(500, 2000);
        Self { k, num_candidates }
    }
}

impl Default for KnnOptions {
    fn default() -> Self {
        Self {
            k: 200,
            num_candidates: 500,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
