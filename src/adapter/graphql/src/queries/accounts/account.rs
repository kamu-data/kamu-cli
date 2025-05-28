// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{
    AccountNotFoundByIdError,
    AccountService,
    AccountServiceExt,
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
};
use kamu_auth_rebac::{RebacService, RebacServiceExt};
use tokio::sync::OnceCell;

use super::AccountFlows;
use crate::prelude::*;
use crate::utils::check_logged_account_id_match;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Account {
    account_id: AccountID<'static>,
    account_name: AccountName<'static>,
    full_account_info: OnceCell<Box<kamu_accounts::Account>>,
}

#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
pub enum AccountType {
    User,
    Organization,
}

#[Object]
impl Account {
    #[graphql(skip)]
    pub(crate) fn new(account_id: AccountID<'static>, account_name: AccountName<'static>) -> Self {
        Self {
            account_id,
            account_name,
            full_account_info: OnceCell::new(),
        }
    }

    #[graphql(skip)]
    pub(crate) fn from_account(account: kamu_accounts::Account) -> Self {
        Self {
            account_id: AccountID::from(account.id.clone()),
            account_name: account.account_name.clone().into(),
            full_account_info: OnceCell::new_with(Some(Box::new(account))),
        }
    }

    #[graphql(skip)]
    pub(crate) async fn from_account_id(
        ctx: &Context<'_>,
        account_id: odf::AccountID,
    ) -> Result<Self, InternalError> {
        let account_service = from_catalog_n!(ctx, dyn AccountService);

        let account_name = account_service
            .find_account_name_by_id(&account_id)
            .await?
            .ok_or_else(|| AccountNotFoundByIdError {
                account_id: account_id.clone(),
            })
            .int_err()?;

        Ok(Self::new(account_id.into(), account_name.into()))
    }

    #[graphql(skip)]
    pub(crate) async fn from_account_name(
        ctx: &Context<'_>,
        account_name: odf::AccountName,
    ) -> Result<Option<Self>, InternalError> {
        let account_service = from_catalog_n!(ctx, dyn AccountService);

        let maybe_account = account_service.account_by_name(&account_name).await?;

        Ok(maybe_account.map(Self::from_account))
    }

    #[graphql(skip)]
    pub(crate) async fn from_dataset_alias(
        ctx: &Context<'_>,
        alias: &odf::DatasetAlias,
    ) -> Result<Option<Self>, InternalError> {
        if alias.is_multi_tenant() {
            // Safety: In multi-tenant, we have a name.
            let account_name = alias.account_name.as_ref().unwrap().clone();

            Ok(Self::from_account_name(ctx, account_name).await?)
        } else {
            let current_account_subject = from_catalog_n!(ctx, CurrentAccountSubject);

            Ok(Some(match current_account_subject.as_ref() {
                CurrentAccountSubject::Anonymous(_) => Self::new(
                    DEFAULT_ACCOUNT_ID.clone().into(),
                    DEFAULT_ACCOUNT_NAME.clone().into(),
                ),
                CurrentAccountSubject::Logged(l) => {
                    Self::new(l.account_id.clone().into(), l.account_name.clone().into())
                }
            }))
        }
    }

    #[graphql(skip)]
    async fn resolve_full_account_info(&self, ctx: &Context<'_>) -> Result<kamu_accounts::Account> {
        let account_service = from_catalog_n!(ctx, dyn AccountService);

        let maybe_account_info = account_service.account_by_id(&self.account_id).await?;

        maybe_account_info.ok_or_else(|| {
            GqlError::Gql(
                Error::new("Account not resolved")
                    .extend_with(|_, eev| eev.set("id", self.account_id.to_string())),
            )
        })
    }

    #[graphql(skip)]
    #[inline]
    async fn get_full_account_info<'a>(
        &'a self,
        ctx: &Context<'_>,
    ) -> Result<&'a kamu_accounts::Account> {
        self.full_account_info
            .get_or_try_init(async || self.resolve_full_account_info(ctx).await.map(Box::new))
            .await
            .map(AsRef::as_ref)
    }

    #[graphql(skip)]
    pub(crate) fn account_name_internal(&self) -> &AccountName {
        &self.account_name
    }

    #[graphql(skip)]
    pub(crate) fn account_id_internal(&self) -> &odf::AccountID {
        &self.account_id
    }

    /// Unique and stable identifier of this account
    async fn id(&self) -> &AccountID {
        &self.account_id
    }

    /// Symbolic account name
    async fn account_name(&self) -> &AccountName {
        &self.account_name
    }

    /// Account name to display
    async fn display_name<'a>(&'a self, ctx: &Context<'_>) -> Result<AccountDisplayName<'a>> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok((&full_account_info.display_name).into())
    }

    /// Account type
    async fn account_type(&self, ctx: &Context<'_>) -> Result<AccountType> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok(match full_account_info.account_type {
            kamu_accounts::AccountType::User => AccountType::User,
            kamu_accounts::AccountType::Organization => AccountType::Organization,
        })
    }

    /// Email address
    async fn email(&self, ctx: &Context<'_>) -> Result<&str> {
        check_logged_account_id_match(ctx, &self.account_id)?;

        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok(full_account_info.email.as_ref())
    }

    /// Avatar URL
    async fn avatar_url(&self, ctx: &Context<'_>) -> Result<&Option<String>> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok(&full_account_info.avatar_url)
    }

    /// Indicates the administrator status
    async fn is_admin(&self, ctx: &Context<'_>) -> Result<bool> {
        let rebac_service = from_catalog_n!(ctx, dyn RebacService);

        Ok(rebac_service
            .is_account_admin(&self.account_id)
            .await
            .int_err()?)
    }

    /// Access to the flow configurations of this account
    async fn flows(&self, ctx: &Context<'_>) -> Result<AccountFlows> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok(AccountFlows::new(full_account_info))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
