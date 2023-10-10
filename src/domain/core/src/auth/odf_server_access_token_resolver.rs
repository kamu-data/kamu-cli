// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::component;
use url::Url;

///////////////////////////////////////////////////////////////////////////////

pub trait OdfServerAccessTokenResolver: Send + Sync {
    fn resolve_odf_dataset_access_token(&self, odf_dataset_http_url: &Url) -> Option<String>;
}

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct DummyOdfServerAccessTokenResolver {}

impl DummyOdfServerAccessTokenResolver {
    pub fn new() -> Self {
        Self {}
    }
}

pub const DUMMY_ACCESS_TOKEN: &str = "some-token";

#[async_trait::async_trait]
impl OdfServerAccessTokenResolver for DummyOdfServerAccessTokenResolver {
    fn resolve_odf_dataset_access_token(&self, _odf_dataset_http_url: &Url) -> Option<String> {
        Some(DUMMY_ACCESS_TOKEN.to_string())
    }
}

///////////////////////////////////////////////////////////////////////////////
