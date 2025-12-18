// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{
    FullTextEntityId,
    FullTextEntitySchemaName,
    FullTextSearchFieldPath,
    FullTextSearchRequest,
    FullTextSearchResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FullTextSearchService: Send + Sync {
    async fn health(
        &self,
        ctx: FullTextSearchContext<'_>,
    ) -> Result<serde_json::Value, InternalError>;

    async fn search(
        &self,
        ctx: FullTextSearchContext<'_>,
        req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError>;

    async fn find_document_by_id(
        &self,
        ctx: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        id: &FullTextEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError>;

    async fn bulk_update(
        &self,
        ctx: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        operations: Vec<FullTextUpdateOperation>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy)]
pub struct FullTextSearchContext<'a> {
    pub catalog: &'a dill::Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum FullTextUpdateOperation {
    Index {
        id: FullTextEntityId,
        doc: serde_json::Value,
    },
    Update {
        id: FullTextEntityId,
        doc: serde_json::Value,
    },
    Delete {
        id: FullTextEntityId,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents the state of a field extraction in incremental indexing
pub enum FullTextSearchFieldUpdate<T> {
    /// Field was not present in the interval (no corresponding event)
    Absent,
    /// Field was present but empty (event exists, but data cleared)
    Empty,
    /// Field was present with data
    Present(T),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Helper to conditionally insert field based on update state
pub fn insert_full_text_incremental_update_field<T: serde::Serialize>(
    incremental_update: &mut serde_json::Map<String, serde_json::Value>,
    field_path: FullTextSearchFieldPath,
    field_update: FullTextSearchFieldUpdate<T>,
) {
    match field_update {
        FullTextSearchFieldUpdate::Absent => {}
        FullTextSearchFieldUpdate::Empty => {
            incremental_update.insert(field_path.to_string(), serde_json::json!(null));
        }
        FullTextSearchFieldUpdate::Present(v) => {
            incremental_update.insert(field_path.to_string(), serde_json::json!(v));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
