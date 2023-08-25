// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{BoxedError, InternalError};
use thiserror::Error;

use super::{AccountInfo, InvalidCredentialsError, RejectedCredentialsError};

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AuthenticationService: Sync + Send {
    async fn login(
        &self,
        login_method: &str,
        login_credentials_json: String,
    ) -> Result<LoginResponse, LoginError>;

    async fn get_account_info(
        &self,
        access_token: String,
    ) -> Result<AccountInfo, GetAccountInfoError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct LoginResponse {
    pub access_token: String,
    pub account_info: AccountInfo,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum LoginError {
    #[error(transparent)]
    UnknownMethod(
        #[from]
        #[backtrace]
        UnknownLoginMethodError,
    ),

    #[error(transparent)]
    InvalidCredentials(
        #[from]
        #[backtrace]
        InvalidCredentialsError,
    ),

    #[error(transparent)]
    RejectedCredentials(
        #[from]
        #[backtrace]
        RejectedCredentialsError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Debug, Error)]
#[error("Unknown login method {method}")]
pub struct UnknownLoginMethodError {
    pub method: String,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetAccountInfoError {
    #[error(transparent)]
    AccessToken(AccessTokenError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
pub enum AccessTokenError {
    #[error("Invalid access token")]
    Invalid(#[source] BoxedError),

    #[error("Expired access token")]
    Expired,
}

///////////////////////////////////////////////////////////////////////////////
