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

    async fn search(
        &self,
        _: SearchContext<'_>,
        req: SearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        self.search_repo.search(req).await
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
