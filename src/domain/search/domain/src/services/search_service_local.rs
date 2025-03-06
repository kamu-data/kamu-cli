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
pub trait SearchServiceLocal: Send + Sync {
    /// Search for datasets using a natural language prompt
    async fn search_natural_language(
        &self,
        prompt: &str,
        options: SearchNatLangOpts,
    ) -> Result<SearchLocalNatLangResult, SearchLocalNatLangError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct SearchNatLangOpts {
    pub skip: Option<usize>,
    pub limit: Option<usize>,
    pub min_score: f32,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchLocalNatLangResult {
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
pub enum SearchLocalNatLangError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}
