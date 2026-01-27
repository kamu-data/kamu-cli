// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn SearchService)]
pub struct SearchServiceImpl {
    search_repo: Arc<dyn SearchRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SearchServiceImpl {
    fn ensure_unrestricted_context<'a>(
        &self,
        ctx: SearchContext<'a>,
    ) -> Result<SearchContext<'a>, SearchError> {
        match ctx.security {
            SearchSecurityContext::Unrestricted => Ok(ctx),
            _ => Err(SearchError::Unauthorized(odf::AccessError::Unauthorized(
                "Unrestricted security context required".into(),
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SearchService for SearchServiceImpl {
    async fn health(&self, ctx: SearchContext<'_>) -> Result<serde_json::Value, SearchError> {
        self.ensure_unrestricted_context(ctx)?;

        self.search_repo.health().await.map_err(Into::into)
    }

    async fn find_document_by_id(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, SearchError> {
        self.ensure_unrestricted_context(ctx)?;

        self.search_repo
            .find_document_by_id(schema_name, id)
            .await
            .map_err(Into::into)
    }

    async fn bulk_update(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), SearchError> {
        self.ensure_unrestricted_context(ctx)?;

        self.search_repo
            .bulk_update(schema_name, operations)
            .await
            .map_err(Into::into)
    }

    async fn listing_search(
        &self,
        ctx: SearchContext<'_>,
        req: ListingSearchRequest,
    ) -> Result<SearchResponse, SearchError> {
        self.search_repo
            .listing_search(ctx.security, req)
            .await
            .map_err(Into::into)
    }

    async fn text_search(
        &self,
        ctx: SearchContext<'_>,
        req: TextSearchRequest,
    ) -> Result<SearchResponse, SearchError> {
        self.search_repo
            .text_search(ctx.security, req)
            .await
            .map_err(Into::into)
    }

    async fn vector_search(
        &self,
        ctx: SearchContext<'_>,
        req: VectorSearchRequest,
    ) -> Result<SearchResponse, SearchError> {
        self.search_repo
            .vector_search(ctx.security, req)
            .await
            .map_err(Into::into)
    }

    async fn hybrid_search(
        &self,
        ctx: SearchContext<'_>,
        req: HybridSearchRequest,
    ) -> Result<SearchResponse, SearchError> {
        self.search_repo
            .hybrid_search(ctx.security, req)
            .await
            .map_err(Into::into)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
