// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SearchService: Send + Sync {
    async fn health(&self, ctx: SearchContext<'_>) -> Result<serde_json::Value, SearchError>;

    async fn find_document_by_id(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, SearchError>;

    async fn bulk_update(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), SearchError>;

    async fn listing_search(
        &self,
        ctx: SearchContext<'_>,
        req: ListingSearchRequest,
    ) -> Result<SearchResponse, SearchError>;

    async fn text_search(
        &self,
        ctx: SearchContext<'_>,
        req: TextSearchRequest,
    ) -> Result<SearchResponse, SearchError>;

    async fn vector_search(
        &self,
        ctx: SearchContext<'_>,
        req: VectorSearchRequest,
    ) -> Result<SearchResponse, SearchError>;

    async fn hybrid_search(
        &self,
        ctx: SearchContext<'_>,
        req: HybridSearchRequest,
    ) -> Result<SearchResponse, SearchError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct SearchContext<'a> {
    pub catalog: &'a dill::Catalog,
    pub security: SearchSecurityContext,
}

impl<'a> SearchContext<'a> {
    pub fn unrestricted(catalog: &'a dill::Catalog) -> Self {
        Self {
            catalog,
            security: SearchSecurityContext::Unrestricted,
        }
    }
}

#[derive(Clone)]
pub enum SearchSecurityContext {
    /// No security filtering (e.g., for internal system use, admin mode)
    Unrestricted,

    /// Apply security filtering based on the provided principal IDs
    Restricted { current_principal_ids: Vec<String> },

    /// Anonymous context with no principals
    Anonymous,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum SearchIndexUpdateOperation {
    Index {
        id: SearchEntityId,
        doc: serde_json::Value,
    },
    Update {
        id: SearchEntityId,
        doc: serde_json::Value,
    },
    Delete {
        id: SearchEntityId,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    #[error(transparent)]
    Unauthorized(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
