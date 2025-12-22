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
pub struct ElasticSearchClientConfig {
    pub url: url::Url,
    pub password: Option<String>,
    pub timeout_secs: u64,
    pub enable_compression: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ElasticSearchRepositoryConfig {
    pub index_prefix: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ElasticSearchContainerConfig {
    pub image: String,
    pub start_timeout: std::time::Duration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
