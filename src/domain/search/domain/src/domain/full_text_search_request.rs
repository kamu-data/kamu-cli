// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MAX_SEARCH_PAGE_SIZE: usize = 10000;
pub const DEFAULT_SEARCH_PAGE_SIZE: usize = 10;

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

    /// Options
    pub options: FullTextSearchOptions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct FullTextSearchOptions {
    pub enable_explain: bool,
    pub enable_highlighting: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FullTextPageSpec {
    pub limit: usize,
    pub offset: usize,
}

impl FullTextPageSpec {
    pub fn max(offset: usize) -> Self {
        Self {
            limit: MAX_SEARCH_PAGE_SIZE,
            offset,
        }
    }
}

impl From<Option<PaginationOpts>> for FullTextPageSpec {
    fn from(pagination: Option<PaginationOpts>) -> Self {
        match pagination {
            Some(p) => Self {
                limit: p.limit,
                offset: p.offset,
            },
            None => Self::max(0),
        }
    }
}

impl Default for FullTextPageSpec {
    fn default() -> Self {
        Self {
            limit: DEFAULT_SEARCH_PAGE_SIZE,
            offset: 0,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
