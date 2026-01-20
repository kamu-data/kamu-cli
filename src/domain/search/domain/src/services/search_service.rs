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
    async fn health(&self, ctx: SearchContext<'_>) -> Result<serde_json::Value, InternalError>;

    async fn search(
        &self,
        ctx: SearchContext<'_>,
        req: SearchRequest,
    ) -> Result<SearchResponse, InternalError>;

    async fn vector_search(
        &self,
        ctx: SearchContext<'_>,
        req: VectorSearchRequest,
    ) -> Result<SearchResponse, InternalError>;

    async fn find_document_by_id(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError>;

    async fn bulk_update(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy)]
pub struct SearchContext<'a> {
    pub catalog: &'a dill::Catalog,
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

/// Represents the state of a field extraction in incremental indexing
pub enum SearchFieldUpdate<T> {
    /// Field was not present in the interval (no corresponding event)
    Absent,
    /// Field was present but empty (event exists, but data cleared)
    Empty,
    /// Field was present with data
    Present(T),
}

impl<T> SearchFieldUpdate<T> {
    pub fn is_present(&self) -> bool {
        matches!(self, SearchFieldUpdate::Present(_))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Helper to conditionally insert field based on update state
pub fn insert_search_incremental_update_field<T: serde::Serialize>(
    incremental_update: &mut serde_json::Map<String, serde_json::Value>,
    field_path: SearchFieldPath,
    field_update: SearchFieldUpdate<T>,
) {
    match field_update {
        SearchFieldUpdate::Absent => {}
        SearchFieldUpdate::Empty => {
            incremental_update.insert(field_path.to_string(), serde_json::json!(null));
        }
        SearchFieldUpdate::Present(v) => {
            incremental_update.insert(field_path.to_string(), serde_json::json!(v));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
