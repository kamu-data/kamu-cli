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
pub trait DatasetCredentialsResolver: Send + Sync {
    async fn resolve_dataset_credentials(
        &self,
        dataset_http_url: &Url,
    ) -> Result<DatasetCredentials, ResolveDatasetCredentialsError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum DatasetCredentials {
    AccessToken(DatasetAccessToken),
}

#[derive(Debug, Clone)]
pub struct DatasetAccessToken {
    pub token: String,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResolveDatasetCredentialsError {
    #[error(transparent)]
    LoginRequired(DatasetLoginRequiredError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("Access to '{server_url}' requires authentication")]
pub struct DatasetLoginRequiredError {
    pub server_url: Url,
}

///////////////////////////////////////////////////////////////////////////////
