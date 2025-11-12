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
    async fn health(&self) -> Result<serde_json::Value, InternalError> {
        self.full_text_repo.health().await
    }

    async fn register_entity_schema(
        &self,
        _entity: FullTextSearchEntitySchema,
    ) -> Result<(), InternalError> {
        unimplemented!()
    }

    async fn index_bulk(
        &self,
        _kind: FullTextEntityKind,
        _docs: Vec<(String, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        unimplemented!()
    }

    async fn delete_bulk(
        &self,
        _kind: FullTextEntityKind,
        _ids: Vec<String>,
    ) -> Result<(), InternalError> {
        unimplemented!()
    }

    async fn search(
        &self,
        _ctx: &FullTextSearchContext,
        _req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
