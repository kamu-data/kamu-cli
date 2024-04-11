// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{BoxedError, InternalError};
use opendatafabric::{AccountID, AccountName};
use random_names::get_random_name;
use thiserror::Error;

use super::{InvalidCredentialsError, RejectedCredentialsError};
use crate::Account;

///////////////////////////////////////////////////////////////////////////////

pub const ENV_VAR_KAMU_JWT_SECRET: &str = "KAMU_JWT_SECRET";

///////////////////////////////////////////////////////////////////////////////

pub fn set_random_jwt_secret() {
    let random_jwt_secret = get_random_name(None, 64);

    std::env::set_var(ENV_VAR_KAMU_JWT_SECRET, random_jwt_secret);
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AuthenticationService: Sync + Send {
    fn supported_login_methods(&self) -> Vec<&'static str>;

    async fn login(
        &self,
        login_method: &str,
        login_credentials_json: String,
    ) -> Result<LoginResponse, LoginError>;

    async fn account_by_token(&self, access_token: String) -> Result<Account, GetAccountInfoError>;

    async fn account_by_id(&self, account_id: &AccountID)
        -> Result<Option<Account>, InternalError>;

    async fn account_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<Account>, InternalError>;

    async fn find_account_id_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<AccountID>, InternalError>;

    async fn find_account_name_by_id(
        &self,
        account_id: &AccountID,
    ) -> Result<Option<AccountName>, InternalError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct LoginResponse {
    pub access_token: String,
    pub account_id: AccountID,
    pub account_name: AccountName,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum LoginError {
    #[error(transparent)]
    UnsupportedMethod(
        #[from]
        #[backtrace]
        UnsupportedLoginMethodError,
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

    #[error("Credentials are already used by an existing account")]
    DuplicateCredentials,

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

#[derive(Debug, Error)]
#[error("Unsupported login method '{method}'")]
pub struct UnsupportedLoginMethodError {
    pub method: String,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetAccountInfoError {
    #[error(transparent)]
    AccessToken(AccessTokenError),

    #[error("Account pointed by the token could not be resolved")]
    AccountUnresolved,

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
