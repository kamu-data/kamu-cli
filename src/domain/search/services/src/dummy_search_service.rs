// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SearchService)]
pub struct DummySearchService {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SearchService for DummySearchService {
    async fn health(&self, _: SearchContext<'_>) -> Result<serde_json::Value, InternalError> {
        Ok(serde_json::json!({
            "status": "ok",
            "details": "This is a dummy search service"
        }))
    }

    async fn listing_search(
        &self,
        _: SearchContext<'_>,
        _req: ListingSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        Ok(SearchResponse {
            took_ms: 0,
            timeout: false,
            total_hits: Some(0),
            hits: vec![],
        })
    }

    async fn text_search(
        &self,
        _: SearchContext<'_>,
        _req: TextSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        Ok(SearchResponse {
            took_ms: 0,
            timeout: false,
            total_hits: Some(0),
            hits: vec![],
        })
    }

    async fn vector_search(
        &self,
        _: SearchContext<'_>,
        _req: VectorSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        Ok(SearchResponse {
            took_ms: 0,
            timeout: false,
            total_hits: None,
            hits: vec![],
        })
    }

    async fn hybrid_search(
        &self,
        _ctx: SearchContext<'_>,
        _req: HybridSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        Ok(SearchResponse {
            took_ms: 0,
            timeout: false,
            total_hits: None,
            hits: vec![],
        })
    }

    async fn find_document_by_id(
        &self,
        _ctx: SearchContext<'_>,
        _schema_name: SearchEntitySchemaName,
        _id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError> {
        Ok(None)
    }

    async fn bulk_update(
        &self,
        _ctx: SearchContext<'_>,
        _schema_name: SearchEntitySchemaName,
        _operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), InternalError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
