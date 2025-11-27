// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ElasticSearchFullTextSearchConfig {
    pub url: url::Url,
    pub password: Option<String>,
    pub index_prefix: String,
    pub timeout_secs: u64,
    pub enable_compression: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ElasticSearchFullTextSearchContainerConfig {
    pub image: String,
    pub start_timeout: std::time::Duration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
