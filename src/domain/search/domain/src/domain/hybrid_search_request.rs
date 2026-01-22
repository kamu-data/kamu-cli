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
    ProvidedEmbedding { vector: Vec<f32> },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct HybridSearchOptions {
    pub rrf_rank_window_size: usize,
    pub rrf_rank_constant: usize,
    pub knn_k: usize,
    pub knn_num_candidates: usize,
    pub enable_explain: bool,
}

impl Default for HybridSearchOptions {
    fn default() -> Self {
        Self {
            rrf_rank_window_size: 100,
            rrf_rank_constant: 60,
            knn_k: 200,
            knn_num_candidates: 1000,
            enable_explain: false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
