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

#[derive(Debug, Default)]
pub struct TextSearchRequest {
    /// Free-text prompt
    pub prompt: Option<String>,

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
    pub options: TextSearchOptions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct TextSearchOptions {
    pub enable_explain: bool,
    pub enable_highlighting: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
