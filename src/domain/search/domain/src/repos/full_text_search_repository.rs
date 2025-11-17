// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FullTextSearchRepository: Send + Sync {
    async fn health(&self) -> Result<serde_json::Value, InternalError>;

    async fn ensure_entity_index(
        &self,
        entity_schema: &FullTextSearchEntitySchema,
    ) -> Result<(), FullTextSearchEnsureEntityIndexError>;

    async fn total_documents(&self) -> Result<u64, InternalError>;

    async fn documents_of_kind(&self, kind: FullTextEntityKind) -> Result<u64, InternalError>;

    async fn index_bulk(
        &self,
        kind: FullTextEntityKind,
        docs: Vec<(String, serde_json::Value)>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FullTextSearchEntitySchema {
    pub entity_kind: FullTextEntityKind,
    pub version: u32,
    pub fields: &'static [FullTextSchemaField],
    pub upgrade_mode: FullTextSearchEntitySchemaUpgradeMode,
}

pub type FullTextEntityKind = &'static str;

#[derive(Debug)]
pub struct FullTextSchemaField {
    pub path: FullTestSearchFieldPath,
    pub role: FullTextSchemaFieldRole,
}

pub type FullTestSearchFieldPath = &'static str;

#[derive(Debug, Clone, Copy)]
pub enum FullTextSchemaFieldRole {
    Prose {
        enable_positions: bool,
    },
    Identifier {
        hierarchical: bool,
        enable_ngrams: bool,
    },
    Keyword,
    DateTime,
    // TODO: Add more field roles as needed, e.g., Numeric, Boolean,
}

#[derive(Debug, Clone, Copy)]
pub enum FullTextSearchEntitySchemaUpgradeMode {
    /// Try to preserve existing data via reindexing into new schema
    Reindex,

    /// Existing data won't be preserved, new empty index will be created
    BreakingRecreate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum FullTextSearchEnsureEntityIndexError {
    #[error(
        "Entity index schema drift detected for entity '{entity_kind}', version {version}: \
         expected mapping hash '{expected_hash}', actual mapping hash '{actual_hash}'"
    )]
    SchemaDriftDetected {
        entity_kind: FullTextEntityKind,
        version: u32,
        alias: String,
        index: String,
        expected_hash: String,
        actual_hash: String,
    },

    #[error(
        "Attempted to downgrade entity index for entity '{entity_kind}' from version \
         {existing_version} to {attempted_version}"
    )]
    DowngradeAttempted {
        entity_kind: FullTextEntityKind,
        existing_version: u32,
        alias: String,
        index: String,
        attempted_version: u32,
    },

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
