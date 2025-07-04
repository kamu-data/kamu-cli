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

use crate::{Account, CreateAccountError, Password};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CreateAccountUseCase: Send + Sync {
    async fn execute(
        &self,
        creator_account: &Account,
        account_name: &odf::AccountName,
        options: CreateAccountUseCaseOptions,
    ) -> Result<Account, CreateAccountError>;

    async fn execute_multi_wallet_accounts(
        &self,
        wallet_addresses: HashSet<DidPkh>,
    ) -> Result<Vec<Account>, CreateMultiWalletAccountsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(bon::Builder, Default)]
pub struct CreateAccountUseCaseOptions {
    pub email: Option<Email>,
    pub password: Option<Password>,
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
