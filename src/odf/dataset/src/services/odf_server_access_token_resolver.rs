// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DUMMY_ODF_ACCESS_TOKEN: &str = "some-odf-token";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
pub trait OdfServerAccessTokenResolver: Send + Sync {
    fn resolve_odf_dataset_access_token(&self, odf_dataset_http_url: &Url) -> Option<String>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "testing")]
impl MockOdfServerAccessTokenResolver {
    pub fn empty() -> Self {
        let mut mock_odf_server_access_token_resolver = MockOdfServerAccessTokenResolver::new();
        mock_odf_server_access_token_resolver
            .expect_resolve_odf_dataset_access_token()
            .returning(|_| None);
        mock_odf_server_access_token_resolver
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn OdfServerAccessTokenResolver)]
pub struct DummyOdfServerAccessTokenResolver {}

#[async_trait::async_trait]
impl OdfServerAccessTokenResolver for DummyOdfServerAccessTokenResolver {
    fn resolve_odf_dataset_access_token(&self, _odf_dataset_http_url: &Url) -> Option<String> {
        Some(DUMMY_ODF_ACCESS_TOKEN.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
