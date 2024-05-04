// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::{AccountID, AccountName};
use thiserror::Error;

use super::{InvalidCredentialsError, RejectedCredentialsError};
use crate::{AccountDisplayName, AccountType};

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AuthenticationProvider: Sync + Send {
    fn provider_name(&self) -> &'static str;

    fn generate_id(&self, account_name: &AccountName) -> AccountID;

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ProviderLoginResponse {
    pub account_name: AccountName,
    pub email: Option<String>,
    pub display_name: AccountDisplayName,
    pub account_type: AccountType,
    pub avatar_url: Option<String>,
    pub provider_identity_key: String,
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
