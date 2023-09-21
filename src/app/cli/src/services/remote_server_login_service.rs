// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::component;
use internal_error::InternalError;
use thiserror::Error;
use url::Url;

use crate::RemoteServerCredentials;

////////////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_LOGIN_URL: &str = "http://localhost:4200";

////////////////////////////////////////////////////////////////////////////////////////

struct RemoteServerLoginService {}

#[component(pub)]
impl RemoteServerLoginService {
    pub fn new() -> Self {
        Self {}
    }

    #[allow(dead_code)]
    pub async fn obtain_login_credentials(
        &self,
        _server_url: &Url,
    ) -> Result<RemoteServerCredentials, InternalError> {
        // TODO
        unimplemented!()
    }

    #[allow(dead_code)]
    pub async fn validate_login_credentials(
        &self,
        _credentials: RemoteServerCredentials,
    ) -> Result<(), RemoteServerValidateLoginError> {
        // TODO
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RemoteServerValidateLoginError {
    #[error(transparent)]
    ExpiredCredentials(RemoteServerExpiredCredentialsError),

    #[error(transparent)]
    InvalidCredentials(RemoteServerInvalidCredentialsError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("Credentials for '{server_url}' remote server have expired. Please re-run `kamu login`")]
pub struct RemoteServerExpiredCredentialsError {
    server_url: Url,
}

#[derive(Debug, Error)]
#[error("Credentials for '{server_url}' are invalid. Please re-run `kamu login`")]
pub struct RemoteServerInvalidCredentialsError {
    server_url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////
