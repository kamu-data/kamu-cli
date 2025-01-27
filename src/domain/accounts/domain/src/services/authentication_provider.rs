// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use email_utils::Email;
use internal_error::InternalError;
use thiserror::Error;

use super::{InvalidCredentialsError, NoPrimaryEmailError, RejectedCredentialsError};
use crate::{AccountDisplayName, AccountType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AuthenticationProvider: Sync + Send {
    fn provider_name(&self) -> &'static str;

    fn generate_id(&self, account_name: &odf::AccountName) -> odf::AccountID;

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ProviderLoginResponse {
    pub account_name: odf::AccountName,
    pub email: Email,
    pub display_name: AccountDisplayName,
    pub account_type: AccountType,
    pub avatar_url: Option<String>,
    pub provider_identity_key: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    NoPrimaryEmail(
        #[from]
        #[backtrace]
        NoPrimaryEmailError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
