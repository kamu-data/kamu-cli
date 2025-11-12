// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FullTextEntityId = String;
pub type FullTextEntitySchemaName = &'static str;
pub type FullTextSearchFieldPath = &'static str;

pub const FULL_TEXT_SEARCH_ALIAS_TITLE: &str = "title";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FullTextSearchEntitySchema {
    pub schema_name: FullTextEntitySchemaName,
    pub version: u32,
    pub fields: &'static [FullTextSchemaField],
    pub title_field: FullTextSearchFieldPath,
    pub upgrade_mode: FullTextSearchEntitySchemaUpgradeMode,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FullTextSchemaField {
    pub path: FullTextSearchFieldPath,
    pub role: FullTextSchemaFieldRole,
}

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub enum FullTextSearchEntitySchemaUpgradeMode {
    /// Try to preserve existing data via reindexing into new schema
    Reindex,

    /// Existing data won't be preserved, new empty index will be created
    BreakingRecreate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
