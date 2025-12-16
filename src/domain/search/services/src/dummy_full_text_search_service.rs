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
#[dill::interface(dyn FullTextSearchService)]
pub struct DummyFullTextSearchService {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FullTextSearchService for DummyFullTextSearchService {
    async fn health(
        &self,
        _: FullTextSearchContext<'_>,
    ) -> Result<serde_json::Value, InternalError> {
        Ok(serde_json::json!({
            "status": "ok",
            "details": "This is a dummy full text search service"
        }))
    }

    async fn search(
        &self,
        _: FullTextSearchContext<'_>,
        _req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError> {
        Ok(FullTextSearchResponse {
            took_ms: 0,
            timeout: false,
            total_hits: 0,
            hits: vec![],
        })
    }

    async fn find_document_by_id(
        &self,
        _ctx: FullTextSearchContext<'_>,
        _schema_name: FullTextEntitySchemaName,
        _id: &FullTextEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError> {
        Ok(None)
    }

    async fn bulk_update(
        &self,
        _ctx: FullTextSearchContext<'_>,
        _schema_name: FullTextEntitySchemaName,
        _operations: Vec<FullTextUpdateOperation>,
    ) -> Result<(), InternalError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
