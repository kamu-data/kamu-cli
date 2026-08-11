// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use email_utils::Email;
use internal_error::InternalError;
use odf::metadata::DidPkh;
use url::Url;

use crate::{Account, AccountConfig, CreateAccountError, Password};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This DI marker is used only for tests. When registered,
/// [`CreateAccountUseCase`] derives deterministic DID keypairs from account
/// names. Never register in production.
#[derive(Debug, Clone, Copy)]
pub struct SeedDidsFromNamesInTests;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CreateAccountUseCase: Send + Sync {
    async fn execute(&self, account_config: &AccountConfig) -> Result<Account, CreateAccountError>;

    async fn execute_derived(
        &self,
        creator_account: &Account,
        account_name: &odf::AccountName,
        options: CreateDerivedAccountUseCaseOptions,
    ) -> Result<Account, CreateAccountError>;

    async fn execute_multi_wallet_accounts(
        &self,
        wallet_addresses: HashSet<DidPkh>,
    ) -> Result<Vec<Account>, CreateMultiWalletAccountsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(bon::Builder, Default)]
pub struct CreateDerivedAccountUseCaseOptions {
    pub email: Option<Email>,
    pub password: Option<Password>,
    pub display_name: Option<String>,
    pub avatar_url: Option<Url>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum CreateMultiWalletAccountsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
