// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type SearchEntityId = String;
pub type SearchEntitySchemaName = &'static str;
pub type SearchFieldPath = &'static str;

pub const SEARCH_ALIAS_TITLE: &str = "title";
pub const SEARCH_FIELD_IS_BANNED: &str = "is_banned";
pub const SEARCH_FIELD_SEMANTIC_EMBEDDINGS: &str = "semantic_embeddings";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SearchEntitySchema {
    pub schema_name: SearchEntitySchemaName,
    pub version: u32,
    pub upgrade_mode: SearchEntitySchemaUpgradeMode,
    pub fields: &'static [SearchSchemaField],
    pub title_field: SearchFieldPath,
    pub enable_banning: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SearchEntitySchema {
    pub fn find_embedding_chunks_field(&self) -> Option<&SearchSchemaField> {
        self.fields
            .iter()
            .find(|f| matches!(f.role, SearchSchemaFieldRole::EmbeddingChunks))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct SearchSchemaField {
    pub path: SearchFieldPath,
    pub role: SearchSchemaFieldRole,
}

#[derive(Debug, Clone, Copy)]
pub enum SearchSchemaFieldRole {
    Identifier {
        hierarchical: bool,
        enable_edge_ngrams: bool,
        enable_inner_ngrams: bool,
    },
    Name,
    Description,
    Prose,
    Keyword,
    DateTime,
    Boolean,
    Integer,
    EmbeddingChunks,
    UnprocessedObject,
    // TODO: Add more field roles as needed
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub enum SearchEntitySchemaUpgradeMode {
    /// Try to preserve existing data via reindexing into new schema
    Reindex,

    /// Existing data won't be preserved, new empty index will be created
    BreakingRecreate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
