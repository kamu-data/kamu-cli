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
pub struct HybridSearchRequest {
    /// Natural-language prompt used for lexical and/or semantic retrieval
    pub prompt: String,

    /// Whether to use embeddings
    pub semantic_mode: SemanticSearchMode,

    /// Allowed entity types (empty means all)
    pub entity_schemas: Vec<SearchEntitySchemaName>,

    /// Requested source fields. If empty, only IDs will be returned.
    pub source: SearchRequestSourceSpec,

    /// Structured filter applied to all branches
    pub filter: Option<SearchFilterExpr>,

    /// How many recommendations to return
    pub limit: usize,

    /// Hybrid tuning options
    pub options: HybridSearchOptions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub enum SemanticSearchMode {
    /// Cheapest: lexical-only (BM25)
    #[default]
    Disabled,

    /// Provided embedding
    ProvidedEmbedding { prompt_embedding: Vec<f32> },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct HybridSearchOptions {
    pub rrf: RRFOptions,
    pub knn: KnnOptions,
    pub enable_explain: bool,
}

impl HybridSearchOptions {
    pub fn for_limit(limit: usize) -> Self {
        Self {
            rrf: RRFOptions::for_limit(limit),
            knn: KnnOptions::for_limit(limit),
            enable_explain: false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct RRFOptions {
    pub rank_window_size: usize,
    pub rank_constant: usize,
}

impl RRFOptions {
    pub fn for_limit(limit: usize) -> Self {
        Self {
            rank_window_size: (10 * limit).clamp(50, 500),
            rank_constant: 60,
        }
    }
}

impl Default for RRFOptions {
    fn default() -> Self {
        Self {
            rank_window_size: 100,
            rank_constant: 60,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
