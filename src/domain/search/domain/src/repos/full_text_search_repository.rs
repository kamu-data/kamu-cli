// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FullTextEntityId = String;

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

#[derive(Debug, Clone)]
pub struct FullTextSearchEntitySchema {
    pub schema_name: FullTextEntitySchemaName,
    pub version: u32,
    pub fields: &'static [FullTextSchemaField],
    pub title_field: FullTextSearchFieldPath,
    pub upgrade_mode: FullTextSearchEntitySchemaUpgradeMode,
}

pub type FullTextEntitySchemaName = &'static str;

#[derive(Debug)]
pub struct FullTextSchemaField {
    pub path: FullTextSearchFieldPath,
    pub role: FullTextSchemaFieldRole,
}

pub type FullTextSearchFieldPath = &'static str;

#[derive(Debug, Clone, Copy)]
pub enum FullTextSchemaFieldRole {
    Identifier {
        hierarchical: bool,
        enable_edge_ngrams: bool,
        enable_inner_ngrams: bool,
    },
    Name,
    Prose {
        enable_positions: bool,
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

pub const FULL_TEXT_SEARCH_ALIAS_TITLE: &str = "title";

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
// Search request model
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct FullTextSearchRequest {
    /// Free-text query
    pub query: Option<String>,

    /// Allowed entity types (empty means all)
    pub entity_schemas: Vec<FullTextEntitySchemaName>,

    /// Requested source fields. If empty, only IDs will be returned.
    pub source: FullTextSearchRequestSourceSpec,

    /// Structured filter
    pub filter: Option<FullTextSearchFilterExpr>,

    /// Sorting specification, Relevance by default
    pub sort: Vec<FullTextSortSpec>,

    /// Pagination specification
    pub page: FullTextPageSpec,

    /// Debug payload enabled
    pub debug: bool,
}

#[derive(Debug, Default)]
pub enum FullTextSearchRequestSourceSpec {
    None,
    #[default]
    All,
    Particular(Vec<FullTextSearchFieldPath>),
    Complex {
        include_patterns: Vec<String>,
        exclude_patterns: Vec<String>,
    },
}

#[derive(Debug)]
pub enum FullTextSearchFilterExpr {
    Field {
        field: FullTextSearchFieldPath,
        op: FullTextSearchFilterOp,
    },
    And(Vec<FullTextSearchFilterExpr>),
    Or(Vec<FullTextSearchFilterExpr>),
    Not(Box<FullTextSearchFilterExpr>),
}

#[derive(Debug)]
pub enum FullTextSearchFilterOp {
    Eq(serde_json::Value),
    // TODO: Add more operators as needed
}

#[derive(Debug, Default)]
pub enum FullTextSortSpec {
    #[default]
    Relevance,
    ByField {
        field: FullTextSearchFieldPath,
        direction: FullTextSortDirection,
        nulls_first: bool,
    },
}

#[derive(Debug)]
pub enum FullTextSortDirection {
    Ascending,
    Descending,
}

#[derive(Debug)]
pub struct FullTextPageSpec {
    pub limit: usize,
    pub offset: usize,
}

impl Default for FullTextPageSpec {
    fn default() -> Self {
        Self {
            limit: 10,
            offset: 0,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Search response model
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct FullTextSearchResponse {
    pub took_ms: u64,
    pub timeout: bool,
    pub total_hits: u64,
    pub hits: Vec<FullTextSearchHit>,
}

#[derive(Debug)]
pub struct FullTextSearchHit {
    pub id: String,
    pub schema_name: FullTextEntitySchemaName,
    pub score: Option<f64>,
    pub highlights: BTreeMap<String, Vec<String>>,
    pub source: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
