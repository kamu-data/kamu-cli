// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;
use url::Url;

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait OdfServerAccessTokenResolver: Send + Sync {
    async fn resolve_odf_dataset_access_token(
        &self,
        odf_dataset_http_url: &Url,
    ) -> Result<String, OdfServerAccessTokenResolveError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum OdfServerAccessTokenResolveError {
    #[error(transparent)]
    LoginRequired(OdfServerLoginRequiredError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("Access to '{odf_server_backend_url}' ODF server requires authentication")]
pub struct OdfServerLoginRequiredError {
    pub odf_server_backend_url: Url,
}

///////////////////////////////////////////////////////////////////////////////
