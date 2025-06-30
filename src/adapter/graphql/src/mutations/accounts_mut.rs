// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::Context;
use kamu_accounts::{
    AccountService,
    AccountServiceExt,
    CreateAccountError,
    CreateAccountUseCase,
    CreateAccountUseCaseOptions,
    CurrentAccountSubject,
};

use crate::mutations::AccountMut;
use crate::prelude::*;
use crate::queries;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountsMut;

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl AccountsMut {
    /// Returns a mutable account by its id
    #[tracing::instrument(level = "info", name = AccountsMut_by_id, skip_all, fields(%account_id))]
    async fn by_id(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID<'_>,
    ) -> Result<Option<AccountMut>> {
        // NOTE: AccountMut' methods handle access verification

        let account_service = from_catalog_n!(ctx, dyn AccountService);

        let account_maybe = account_service.account_by_id(&account_id).await?;
        Ok(account_maybe.map(AccountMut::new))
    }

    /// Returns a mutable account by its name
    #[tracing::instrument(level = "info", name = AccountsMut_by_name, skip_all, fields(%account_name))]
    async fn by_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName<'_>,
    ) -> Result<Option<AccountMut>> {
        // NOTE: AccountMut' methods handle access verification

        let account_service = from_catalog_n!(ctx, dyn AccountService);

        let account_maybe = account_service.account_by_name(&account_name).await?;
        Ok(account_maybe.map(AccountMut::new))
    }

    /// Create a new account
    #[tracing::instrument(level = "info", name = AccountsMut_create_account, skip_all, fields(%account_name, ?email))]
    #[graphql(guard = "LoggedInGuard.and(CanProvisionAccountsGuard)")]
    async fn create_account(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName<'_>,
        email: Option<Email<'_>>,
    ) -> Result<CreateAccountResult> {
        let (current_account_subject, create_account_use_case, account_service) = from_catalog_n!(
            ctx,
            CurrentAccountSubject,
            dyn CreateAccountUseCase,
            dyn AccountService
        );

        let logged_account_id = current_account_subject.account_id();
        let logged_account = account_service
            .get_account_by_id(logged_account_id)
            .await
            .int_err()?;

        match create_account_use_case
            .execute(
                &logged_account,
                account_name.as_ref(),
                CreateAccountUseCaseOptions::builder()
                    .maybe_email(email.map(Into::into))
                    .build(),
            )
            .await
        {
            Ok(created_account) => Ok(CreateAccountResult::Success(CreateAccountSuccess {
                account: queries::Account::from_account(created_account),
            })),
            Err(CreateAccountError::Duplicate(e)) => Ok(
                CreateAccountResult::NonUniqueAccountField(AccountFieldNonUnique {
                    field: e.account_field.to_string(),
                }),
            ),
            Err(e @ CreateAccountError::Internal(_)) => Err(e.int_err().into()),
        }
    }

    #[tracing::instrument(level = "info", name = AccountsMut_create_wallet_accounts, skip_all)]
    #[graphql(guard = "LoggedInGuard.and(CanProvisionAccountsGuard)")]
    async fn create_wallet_accounts(
        &self,
        ctx: &Context<'_>,
        wallet_addresses: Vec<DidPkh>,
    ) -> Result<CreateWalletAccountsResult> {
        let create_account_use_case = from_catalog_n!(ctx, dyn CreateAccountUseCase);
        let wallet_addresses = wallet_addresses.into_iter().map(Into::into).collect();

        let created_accounts = create_account_use_case
            .execute_multi_wallet_accounts(wallet_addresses)
            .await
            .int_err()?;

        Ok(CreateWalletAccountsResult::Success(
            CreateWalletAccountsSuccess {
                accounts: created_accounts
                    .into_iter()
                    .map(queries::Account::from_account)
                    .collect(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CreateAccountResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateAccountResult {
    Success(CreateAccountSuccess),
    NonUniqueAccountField(AccountFieldNonUnique),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateAccountSuccess {
    pub account: queries::Account,
}

#[ComplexObject]
impl CreateAccountSuccess {
    pub async fn message(&self) -> String {
        "Account created".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, SimpleObject)]
#[graphql(complex)]
pub struct AccountFieldNonUnique {
    field: String,
}

#[ComplexObject]
impl AccountFieldNonUnique {
    pub async fn message(&self) -> String {
        format!("Non-unique account field '{}'", self.field)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CreateWalletAccountsResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateWalletAccountsResult {
    Success(CreateWalletAccountsSuccess),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct CreateWalletAccountsSuccess {
    pub accounts: Vec<queries::Account>,
}

#[ComplexObject]
impl CreateWalletAccountsSuccess {
    pub async fn message(&self) -> String {
        "Wallet accounts created".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
