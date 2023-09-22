// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use internal_error::InternalError;
use kamu::domain::CurrentAccountSubject;
use thiserror::Error;
use url::Url;

use crate::RemoteServerCredentials;

////////////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_LOGIN_URL: &str = "http://localhost:4200";

////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteServerLoginService {
    _current_account_subject: Arc<CurrentAccountSubject>,
}

#[component(pub)]
impl RemoteServerLoginService {
    pub fn new(current_account_subject: Arc<CurrentAccountSubject>) -> Self {
        Self {
            _current_account_subject: current_account_subject,
        }
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
