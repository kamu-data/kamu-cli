// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::SearchContext;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetSearchService: Send + Sync {
    async fn vector_search(
        &self,
        ctx: SearchContext<'_>,
        prompt: String,
        limit: usize,
    ) -> Result<DatasetSearchResponse, DatasetSearchError>;

    async fn hybrid_search(
        &self,
        ctx: SearchContext<'_>,
        prompt: String,
        limit: usize,
    ) -> Result<DatasetSearchResponse, DatasetSearchError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default, PartialEq)]
pub struct DatasetSearchResponse {
    pub hits: Vec<DatasetSearchHit>,
    pub total_hits: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatasetSearchHit {
    pub handle: odf::DatasetHandle,
    pub score: f64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum DatasetSearchError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        internal_error::InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
