// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TextSearchRequest {
    /// Text search intent
    pub intent: TextSearchIntent,

    /// Allowed entity types (empty means all)
    pub entity_schemas: Vec<SearchEntitySchemaName>,

    /// Requested source fields. If empty, only IDs will be returned.
    pub source: SearchRequestSourceSpec,

    /// Structured filter
    pub filter: Option<SearchFilterExpr>,

    /// Pagination specification
    pub page: SearchPaginationSpec,

    /// Options
    pub options: TextSearchOptions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum TextSearchIntent {
    /// Full-text search over multiple fields
    FullText {
        prompt: String,
        term_operator: FullTextSearchTermOperator,
        field_relation: FullTextSearchFieldRelation,
    },

    /// Autocomplete / prefix
    Prefix { prompt: String },

    /// Exact phrase
    Phrase { prompt: String, user_slop: u32 },
}

impl TextSearchIntent {
    pub fn make_full_text(prompt: impl Into<String>) -> Self {
        Self::FullText {
            prompt: prompt.into(),
            term_operator: FullTextSearchTermOperator::Or,
            field_relation: FullTextSearchFieldRelation::BestFields,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, strum::Display)]
pub enum FullTextSearchTermOperator {
    /// Terms in the query must all be present
    #[strum(to_string = "and")]
    And,

    /// Any of the terms in the query should be present
    #[strum(to_string = "or")]
    Or,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum FullTextSearchFieldRelation {
    /// Best score among multiple fields is taken
    BestFields,

    /// Scores from multiple fields are summed
    MostFields,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct TextSearchOptions {
    pub enable_explain: bool,
    pub enable_highlighting: bool,
    pub boosting_overrides: TextBoostingOverrides,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct TextBoostingOverrides {
    /// Multiply all prose boosts by this factor (default 1.0)
    pub prose_boost: f32,

    /// Multiple all description boosts by this factor (default 1.0)
    pub description_boost: f32,

    /// Multiply all identifier boosts by this factor (default 1.0)
    pub identifier_boost: f32,

    /// Multiply all name/title boosts by this factor (default 1.0)
    pub name_boost: f32,
}

impl Default for TextBoostingOverrides {
    fn default() -> Self {
        Self {
            prose_boost: 1.0,
            description_boost: 1.0,
            identifier_boost: 1.0,
            name_boost: 1.0,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
