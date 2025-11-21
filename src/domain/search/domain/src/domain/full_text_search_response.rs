// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::FullTextEntitySchemaName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct FullTextSearchResponse {
    pub took_ms: u64,
    pub timeout: bool,
    pub total_hits: u64,
    pub hits: Vec<FullTextSearchHit>,
}

#[derive(Debug)]
pub struct FullTextSearchHit {
    pub id: String,
    pub schema_name: FullTextEntitySchemaName,
    pub score: Option<f64>,
    pub highlights: Option<Vec<FullTextSearchHighlight>>,
    pub source: serde_json::Value,
    pub explanation: Option<serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FullTextSearchHighlight {
    pub field: String,
    pub best_fragment: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
