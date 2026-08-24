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

#[async_trait::async_trait]
pub trait CreateAccountUseCase: Send + Sync {
    async fn execute(
        &self,
        account_config: &AccountConfig,
        resource_id_source: AccountResourceIdSource,
    ) -> Result<Account, CreateAccountError>;

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

/// Chooses how a new account's *resource* id is minted.
///
/// TODO: interim measure -- goes away once account resources are reconciled by
/// the resources framework rather than allocated during account creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountResourceIdSource {
    /// Deterministically seeded from the account name. Used for predefined /
    /// config-driven accounts, whose resource id must stay stable across
    /// restarts and re-registration (the CLI derives the same id for its
    /// pre-workspace subject).
    SeededFromName,
    /// Freshly minted at random. Used for accounts created at runtime, e.g. via
    /// OAuth/Web3 login, where the account name may have been auto-renamed on
    /// collision and carries no identity guarantee.
    Generated,
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
