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

    async fn has_entity_index(&self, kind: &str) -> Result<bool, InternalError>;

    async fn create_entity_index(
        &self,
        entity_schema: &FullTextSearchEntitySchema,
    ) -> Result<(), InternalError>;

    async fn total_documents(&self) -> Result<u64, InternalError>;

    async fn documents_in_index(&self, kind: &str) -> Result<u64, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FullTextSearchEntitySchema {
    pub entity_kind: FullTextEntityKind,
    pub version: u32,
    pub fields: &'static [FullTextSchemaField],
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
