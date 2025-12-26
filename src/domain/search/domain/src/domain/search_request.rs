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
pub struct SearchRequest {
    /// Free-text query
    pub query: Option<String>,

    /// Allowed entity types (empty means all)
    pub entity_schemas: Vec<SearchEntitySchemaName>,

    /// Requested source fields. If empty, only IDs will be returned.
    pub source: SearchRequestSourceSpec,

    /// Structured filter
    pub filter: Option<SearchFilterExpr>,

    /// Sorting specification, Relevance by default
    pub sort: Vec<SearchSortSpec>,

    /// Pagination specification
    pub page: SearchPaginationSpec,

    /// Options
    pub options: SearchOptions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct SearchOptions {
    pub enable_explain: bool,
    pub enable_highlighting: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub enum SearchRequestSourceSpec {
    None,

    #[default]
    All,

    Particular(Vec<SearchFieldPath>),

    Complex {
        include_patterns: Vec<String>,
        exclude_patterns: Vec<String>,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct SearchPaginationSpec {
    pub limit: usize,
    pub offset: usize,
}

impl SearchPaginationSpec {
    pub fn max(offset: usize) -> Self {
        Self {
            limit: MAX_SEARCH_PAGE_SIZE,
            offset,
        }
    }
}

impl From<Option<PaginationOpts>> for SearchPaginationSpec {
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

impl Default for SearchPaginationSpec {
    fn default() -> Self {
        Self {
            limit: DEFAULT_SEARCH_PAGE_SIZE,
            offset: 0,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
