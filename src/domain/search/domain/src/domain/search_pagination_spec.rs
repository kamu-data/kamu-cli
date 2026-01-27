// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MAX_SEARCH_PAGE_SIZE: usize = 10000;
pub const DEFAULT_SEARCH_PAGE_SIZE: usize = 10;

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
