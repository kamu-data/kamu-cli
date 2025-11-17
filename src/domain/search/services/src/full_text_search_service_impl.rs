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
        _req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError> {
        unimplemented!()
    }

    async fn index_bulk(
        &self,
        _: FullTextSearchContext<'_>,
        kind: FullTextEntityKind,
        docs: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        self.full_text_repo.index_bulk(kind, docs).await
    }

    async fn update_bulk(
        &self,
        _: FullTextSearchContext<'_>,
        kind: FullTextEntityKind,
        updates: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        self.full_text_repo.update_bulk(kind, updates).await
    }

    async fn delete_bulk(
        &self,
        _: FullTextSearchContext<'_>,
        kind: FullTextEntityKind,
        ids: Vec<FullTextEntityId>,
    ) -> Result<(), InternalError> {
        self.full_text_repo.delete_bulk(kind, ids).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
