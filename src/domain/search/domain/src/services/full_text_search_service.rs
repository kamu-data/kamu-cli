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

#[async_trait::async_trait]
pub trait FullTextSearchService: Send + Sync {
    async fn health(&self) -> Result<serde_json::Value, InternalError>;

    async fn register_entity_schema(
        &self,
        entity: FullTextSearchEntitySchema,
    ) -> Result<(), InternalError>;

    async fn index_bulk(
        &self,
        kind: FullTextEntityKind,
        docs: Vec<(String, serde_json::Value)>,
    ) -> Result<(), InternalError>;

    async fn delete_bulk(
        &self,
        kind: FullTextEntityKind,
        ids: Vec<String>,
    ) -> Result<(), InternalError>;

    async fn search(
        &self,
        ctx: &FullTextSearchContext,
        req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Entity schema model
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FullTextSearchEntitySchema {
    pub kind: FullTextEntityKind,
    pub fields: Vec<FullTextSchemaField>,
}

pub type FullTextEntityKind = &'static str;

pub struct FullTextSchemaField {
    pub path: FullTestSearchFieldPath,
    pub kind: FullTextSchemaFieldKind,
    pub searchable: bool,
    pub sortable: bool,
    pub filterable: bool,
}

pub type FullTestSearchFieldPath = &'static str;

pub enum FullTextSchemaFieldKind {
    Text,
    Keyword,
    // TODO: Add more field kinds as needed, e.g., Numeric, Date, Boolean,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Search request model
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FullTextSearchContext {
    pub actor_account_id: Option<odf::AccountID>,
}

pub struct FullTextSearchRequest {
    /// Free-text query
    pub query: Option<String>,

    /// Allowed entity types (empty means all)
    pub kinds: Vec<FullTextEntityKind>,

    /// Structured filter
    pub filter: Option<FullTextSearchFilterExpr>,

    /// Sorting specification
    pub sort: Vec<FullTextSortSpec>,

    /// Pagination specification
    pub page: FullTextPageSpec,

    /// Debug payload enabled
    pub debug: bool,
}

pub enum FullTextSearchFilterExpr {
    Field {
        field: FullTestSearchFieldPath,
        op: FullTextSearchFilterOp,
    },
    And(Vec<FullTextSearchFilterExpr>),
    Or(Vec<FullTextSearchFilterExpr>),
    Not(Box<FullTextSearchFilterExpr>),
}

pub enum FullTextSearchFilterOp {
    Eq(serde_json::Value),
    // TODO: Add more operators as needed
}

pub struct FullTextSortSpec {
    pub field: FullTestSearchFieldPath,
    pub direction: FullTextSortDirection,
    pub nulls_first: bool,
}

pub enum FullTextSortDirection {
    Asc,
    Desc,
}

pub struct FullTextPageSpec {
    pub limit: u32,
    pub cursor: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Search response model
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FullTextSearchResponse {
    pub total_hits: Option<u64>,
    pub hits: Vec<FullTextSearchHit>,
    pub next_cursor: Option<String>,
    pub took_ms: Option<u64>,
    pub debug_payload: Option<serde_json::Value>,
}

pub struct FullTextSearchHit {
    pub id: String,
    pub kind: FullTextEntityKind,
    pub score: Option<f32>,
    pub highlights: BTreeMap<String, Vec<String>>,
    pub source: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
