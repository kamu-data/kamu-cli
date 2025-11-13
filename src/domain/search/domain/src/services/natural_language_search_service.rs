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

/// Provides search functionality for datasets and other objects managed by the
/// current node
#[async_trait::async_trait]
pub trait NaturalLanguageSearchService: Send + Sync {
    /// Search for datasets using a natural language prompt.
    ///
    /// Note that currently this API does NOT perform deduplication and
    /// re-ranking, so multiple search hits can refer to the same dataset.
    async fn search_natural_language(
        &self,
        prompt: &str,
        options: SearchNatLangOpts,
    ) -> Result<SearchNatLangResult, SearchNatLangError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SearchNatLangOpts {
    pub limit: usize,
}

impl Default for SearchNatLangOpts {
    fn default() -> Self {
        Self { limit: 10 }
    }
}

// TODO: Support next page token for paginating through results
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchNatLangResult {
    pub datasets: Vec<SearchLocalResultDataset>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchLocalResultDataset {
    pub handle: odf::DatasetHandle,
    pub score: f32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum SearchNatLangError {
    #[error(transparent)]
    NotEnabled(#[from] NatLangSearchNotEnabled),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Debug, thiserror::Error)]
#[error("Natural language search is not enabled")]
pub struct NatLangSearchNotEnabled;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
