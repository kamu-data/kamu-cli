// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{
    AuthenticationService,
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
};
use opendatafabric as odf;
use tokio::sync::OnceCell;

use super::AccountFlows;
use crate::prelude::*;
use crate::utils::check_logged_account_id_match;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Account {
    account_id: AccountID,
    account_name: AccountName,
    full_account_info: OnceCell<kamu_accounts::Account>,
}

#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
pub enum AccountType {
    User,
    Organization,
}

#[Object]
impl Account {
    #[graphql(skip)]
    pub(crate) fn new(account_id: AccountID, account_name: AccountName) -> Self {
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
            full_account_info: OnceCell::new_with(Some(account)),
        }
    }

    #[graphql(skip)]
    pub(crate) async fn from_account_id(
        ctx: &Context<'_>,
        account_id: odf::AccountID,
    ) -> Result<Self, InternalError> {
        let authentication_service = from_catalog::<dyn AuthenticationService>(ctx).unwrap();

        let account_name = authentication_service
            .find_account_name_by_id(&account_id)
            .await?
            .expect("Account must exist");

        Ok(Self::new(account_id.into(), account_name.into()))
    }

    #[graphql(skip)]
    pub(crate) async fn from_account_name(
        ctx: &Context<'_>,
        account_name: odf::AccountName,
    ) -> Result<Option<Self>, InternalError> {
        let authentication_service = from_catalog::<dyn AuthenticationService>(ctx).unwrap();

        let maybe_account = authentication_service
            .account_by_name(&account_name)
            .await?;

        Ok(maybe_account.map(Self::from_account))
    }

    #[graphql(skip)]
    pub(crate) async fn from_dataset_alias(
        ctx: &Context<'_>,
        alias: &odf::DatasetAlias,
    ) -> Result<Option<Self>, InternalError> {
        if alias.is_multi_tenant() {
            Ok(Self::from_account_name(ctx, alias.account_name.as_ref().unwrap().clone()).await?)
        } else {
            let current_account_subject = from_catalog::<CurrentAccountSubject>(ctx).unwrap();

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
        let authentication_service = from_catalog::<dyn AuthenticationService>(ctx).unwrap();

        let maybe_account_info = authentication_service
            .account_by_id(&self.account_id)
            .await?;

        maybe_account_info.ok_or_else(|| {
            GqlError::Gql(
                Error::new("Account not resolved")
                    .extend_with(|_, eev| eev.set("id", self.account_id.to_string())),
            )
        })
    }

    #[graphql(skip)]
    #[inline]
    async fn get_full_account_info(&self, ctx: &Context<'_>) -> Result<&kamu_accounts::Account> {
        self.full_account_info
            .get_or_try_init(|| self.resolve_full_account_info(ctx))
            .await
    }

    #[graphql(skip)]
    pub(crate) fn account_name_internal(&self) -> &AccountName {
        &self.account_name
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
    async fn display_name(&self, ctx: &Context<'_>) -> Result<AccountDisplayName> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok(AccountDisplayName::from(
            full_account_info.display_name.clone(),
        ))
    }

    /// Account type
    async fn account_type(&self, ctx: &Context<'_>) -> Result<AccountType> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok(match full_account_info.account_type {
            kamu_accounts::AccountType::User => AccountType::User,
            kamu_accounts::AccountType::Organization => AccountType::Organization,
        })
    }

    /// Avatar URL
    async fn avatar_url(&self, ctx: &Context<'_>) -> Result<&Option<String>> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok(&full_account_info.avatar_url)
    }

    /// Indicates the administrator status
    async fn is_admin(&self, ctx: &Context<'_>) -> Result<bool> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok(full_account_info.is_admin)
    }

    /// Access to the flow configurations of this account
    async fn flows(&self, ctx: &Context<'_>) -> Result<Option<AccountFlows>> {
        check_logged_account_id_match(ctx, &self.account_id)?;

        Ok(Some(AccountFlows::new(
            self.get_full_account_info(ctx).await?.clone(),
        )))
    }
}

///////////////////////////////////////////////////////////////////////////////
