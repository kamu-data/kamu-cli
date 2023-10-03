// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth::OdfServerAccessTokenResolver;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub OdfServerAccessTokenResolver {}
    #[async_trait::async_trait]
    impl OdfServerAccessTokenResolver for OdfServerAccessTokenResolver {
        async fn resolve_odf_dataset_access_token(
            &self,
            odf_dataset_http_url: &Url,
        ) -> Option<String>;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
