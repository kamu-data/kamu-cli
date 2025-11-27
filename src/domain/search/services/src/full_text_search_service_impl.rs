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
#[dill::interface(dyn FullTextSearchService)]
pub struct FullTextSearchServiceImpl {
    full_text_repo: Arc<dyn FullTextSearchRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FullTextSearchService for FullTextSearchServiceImpl {
    async fn health(
        &self,
        _: FullTextSearchContext<'_>,
    ) -> Result<serde_json::Value, InternalError> {
        self.full_text_repo.health().await
    }

    async fn search(
        &self,
        _: FullTextSearchContext<'_>,
        req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError> {
        self.full_text_repo.search(req).await
    }

    async fn index_bulk(
        &self,
        _: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        docs: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        self.full_text_repo.index_bulk(schema_name, docs).await
    }

    async fn update_bulk(
        &self,
        _: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        updates: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        self.full_text_repo.update_bulk(schema_name, updates).await
    }

    async fn delete_bulk(
        &self,
        _: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        ids: Vec<FullTextEntityId>,
    ) -> Result<(), InternalError> {
        self.full_text_repo.delete_bulk(schema_name, ids).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
