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
pub struct GetDocumentByIdResponse {
    #[serde(rename = "_index")]
    pub index: String,

    #[serde(rename = "_id")]
    pub id: String,

    #[serde(rename = "_version")]
    pub version: Option<u64>,

    pub found: bool,

    #[serde(rename = "_source")]
    pub source: Option<serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
