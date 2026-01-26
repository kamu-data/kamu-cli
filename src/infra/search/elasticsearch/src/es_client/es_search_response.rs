// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize)]
pub struct SearchResponse {
    pub took: u64,
    pub timed_out: bool,
    pub hits: SearchHitsResponse,
}

#[derive(Debug, serde::Deserialize)]
pub struct SearchHitsResponse {
    pub total: Option<SearchHitsTotalResponse>,
    pub hits: Vec<SearchHitResponse>,
}

#[derive(Debug, serde::Deserialize)]
pub struct SearchHitsTotalResponse {
    pub value: u64,
    pub relation: SearchHitsTotalRelation,
}

#[derive(Debug, serde::Deserialize)]
pub enum SearchHitsTotalRelation {
    #[serde(rename = "eq")]
    Eq,
    #[serde(rename = "gte")]
    Gte,
}

#[derive(Debug, serde::Deserialize)]
pub struct SearchHitResponse {
    #[serde(rename = "_index")]
    pub index: String,

    #[serde(rename = "_id")]
    pub id: Option<String>,

    #[serde(rename = "_score")]
    pub score: Option<f64>,

    #[serde(rename = "_source")]
    pub source: Option<serde_json::Value>,

    pub highlight: Option<serde_json::Value>,

    #[serde(rename = "_explanation")]
    pub explanation: Option<serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
