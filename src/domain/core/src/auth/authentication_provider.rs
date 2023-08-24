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

use super::{AccountInfo, InvalidCredentialsError, RejectedCredentialsError};

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AuthenticationProvider: Sync + Send {
    fn login_method(&self) -> &'static str;

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError>;

    async fn get_account_info(
        &self,
        provider_credentials_json: String,
    ) -> Result<AccountInfo, InternalError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ProviderLoginResponse {
    pub provider_credentials_json: String,
    pub account_info: AccountInfo,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ProviderLoginError {
    #[error("Invalid login credentials")]
    InvalidCredentials(
        #[from]
        #[backtrace]
        InvalidCredentialsError,
    ),

    #[error("Rejected credentials")]
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

///////////////////////////////////////////////////////////////////////////////
