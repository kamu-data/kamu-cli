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
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn SearchService)]
pub struct SearchServiceImpl {
    search_repo: Arc<dyn SearchRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SearchService for SearchServiceImpl {
    async fn health(&self, _: SearchContext<'_>) -> Result<serde_json::Value, InternalError> {
        self.search_repo.health().await
    }

    async fn listing_search(
        &self,
        _: SearchContext<'_>,
        req: ListingSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        self.search_repo.listing_search(req).await
    }

    async fn text_search(
        &self,
        _: SearchContext<'_>,
        req: TextSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        self.search_repo.text_search(req).await
    }

    async fn vector_search(
        &self,
        _: SearchContext<'_>,
        req: VectorSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        self.search_repo.vector_search(req).await
    }

    async fn hybrid_search(
        &self,
        _: SearchContext<'_>,
        req: HybridSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        self.search_repo.hybrid_search(req).await
    }

    async fn find_document_by_id(
        &self,
        _: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError> {
        self.search_repo.find_document_by_id(schema_name, id).await
    }

    async fn bulk_update(
        &self,
        _: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), InternalError> {
        self.search_repo.bulk_update(schema_name, operations).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
