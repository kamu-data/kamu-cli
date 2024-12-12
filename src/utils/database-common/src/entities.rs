// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use sqlx::prelude::FromRow;

#[derive(Debug, Copy, Clone)]
pub struct PaginationOpts {
    pub limit: usize,
    pub offset: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
pub struct EventModel {
    pub event_id: i64,
    pub event_payload: sqlx::types::JsonValue,
}

#[derive(Debug, FromRow)]
pub struct ReturningEventModel {
    pub event_id: i64,
}
