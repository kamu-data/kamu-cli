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
pub trait FullTextSearchRepository: Send + Sync {
    async fn health(&self) -> Result<serde_json::Value, InternalError>;

    async fn ensure_entity_index(
        &self,
        schema: &FullTextSearchEntitySchema,
    ) -> Result<(), FullTextSearchEnsureEntityIndexError>;

    async fn total_documents(&self) -> Result<u64, InternalError>;

    async fn documents_of_kind(
        &self,
        schema_name: FullTextEntitySchemaName,
    ) -> Result<u64, InternalError>;

    async fn search(
        &self,
        req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError>;

    async fn index_bulk(
        &self,
        schema_name: FullTextEntitySchemaName,
        docs: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError>;

    async fn update_bulk(
        &self,
        schema_name: FullTextEntitySchemaName,
        updates: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError>;

    async fn delete_bulk(
        &self,
        schema_name: FullTextEntitySchemaName,
        ids: Vec<FullTextEntityId>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum FullTextSearchEnsureEntityIndexError {
    #[error(
        "Entity index schema drift detected for entity '{schema_name}', version {version}: \
         expected mapping hash '{expected_hash}', actual mapping hash '{actual_hash}'"
    )]
    SchemaDriftDetected {
        schema_name: FullTextEntitySchemaName,
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
        schema_name: FullTextEntitySchemaName,
        existing_version: u32,
        alias: String,
        index: String,
        attempted_version: u32,
    },

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
