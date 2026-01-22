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
pub trait SearchRepository: Send + Sync {
    async fn health(&self) -> Result<serde_json::Value, InternalError>;

    async fn ensure_entity_index(
        &self,
        schema: &SearchEntitySchema,
    ) -> Result<(), SearchEnsureEntityIndexError>;

    async fn total_documents(&self) -> Result<u64, InternalError>;

    async fn documents_of_kind(
        &self,
        schema_name: SearchEntitySchemaName,
    ) -> Result<u64, InternalError>;

    async fn listing_search(
        &self,
        req: ListingSearchRequest,
    ) -> Result<SearchResponse, InternalError>;

    async fn text_search(&self, req: TextSearchRequest) -> Result<SearchResponse, InternalError>;

    async fn vector_search(
        &self,
        req: VectorSearchRequest,
    ) -> Result<SearchResponse, InternalError>;

    async fn hybrid_search(
        &self,
        req: HybridSearchRequest,
    ) -> Result<SearchResponse, InternalError>;

    async fn find_document_by_id(
        &self,
        schema_name: SearchEntitySchemaName,
        id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError>;

    async fn bulk_update(
        &self,
        schema_name: SearchEntitySchemaName,
        operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), InternalError>;

    async fn drop_all_schemas(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum SearchEnsureEntityIndexError {
    #[error(
        "Entity index schema drift detected for entity '{schema_name}', version {version}: \
         expected mapping hash '{expected_hash}', actual mapping hash '{actual_hash}'"
    )]
    SchemaDriftDetected {
        schema_name: SearchEntitySchemaName,
        version: u32,
        alias: String,
        index: String,
        expected_hash: String,
        actual_hash: String,
    },

    #[error(
        "Attempted to downgrade entity index for entity '{schema_name}' from version \
         {existing_version} to {attempted_version}"
    )]
    DowngradeAttempted {
        schema_name: SearchEntitySchemaName,
        existing_version: u32,
        alias: String,
        index: String,
        attempted_version: u32,
    },

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
