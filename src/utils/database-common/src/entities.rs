// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct PaginationOpts {
    pub limit: usize,
    pub offset: usize,
}

impl PaginationOpts {
    pub fn all() -> Self {
        PaginationOpts {
            // i64 type is used because sometimes repositories use a conversion to it
            limit: i64::MAX as usize,
            offset: 0,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
