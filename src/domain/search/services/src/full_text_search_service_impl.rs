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

    async fn find_document_by_id(
        &self,
        _: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        id: &FullTextEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError> {
        self.full_text_repo
            .find_document_by_id(schema_name, id)
            .await
    }

    async fn bulk_update(
        &self,
        _: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        operations: Vec<FullTextUpdateOperation>,
    ) -> Result<(), InternalError> {
        self.full_text_repo
            .bulk_update(schema_name, operations)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
