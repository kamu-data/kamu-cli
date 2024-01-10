// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth::DEFAULT_ACCOUNT_NAME;
use kamu_core::CurrentAccountSubject;
use opendatafabric as odf;
use tokio::sync::OnceCell;

use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Account {
    account_id: AccountID,
    account_name: AccountName,
    full_account_info: OnceCell<kamu_core::auth::AccountInfo>,
}

#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
pub enum AccountType {
    User,
    Organization,
}

#[Object]
impl Account {
    #[graphql(skip)]
    fn new(account_id: AccountID, account_name: AccountName) -> Self {
        Self {
            account_id,
            account_name,
            full_account_info: OnceCell::new(),
        }
    }

    #[graphql(skip)]
    pub(crate) fn from_account_info(account_info: kamu_core::auth::AccountInfo) -> Self {
        Self {
            account_id: AccountID::from(account_info.account_id.clone()),
            account_name: account_info.account_name.clone().into(),
            full_account_info: OnceCell::new_with(Some(account_info)),
        }
    }

    #[graphql(skip)]
    pub(crate) fn from_account_name(name: odf::AccountName) -> Self {
        Self::new(AccountID::from(odf::FAKE_ACCOUNT_ID), name.into())
    }

    #[graphql(skip)]
    pub(crate) fn from_dataset_alias(ctx: &Context<'_>, alias: &odf::DatasetAlias) -> Self {
        if alias.is_multi_tenant() {
            Self::new(
                AccountID::from(odf::FAKE_ACCOUNT_ID),
                alias.account_name.as_ref().unwrap().clone().into(),
            )
        } else {
            let current_account_subject =
                from_catalog::<kamu_core::CurrentAccountSubject>(ctx).unwrap();

            match current_account_subject.as_ref() {
                CurrentAccountSubject::Anonymous(_) => Self::new(
                    AccountID::from(odf::FAKE_ACCOUNT_ID),
                    AccountName::from(odf::AccountName::new_unchecked(DEFAULT_ACCOUNT_NAME)),
                ),
                CurrentAccountSubject::Logged(l) => Self::new(
                    AccountID::from(odf::FAKE_ACCOUNT_ID),
                    l.account_name.clone().into(),
                ),
            }
        }
    }

    #[graphql(skip)]
    async fn resolve_full_account_info(
        &self,
        ctx: &Context<'_>,
    ) -> Result<kamu_core::auth::AccountInfo> {
        let authentication_service =
            from_catalog::<dyn kamu_core::auth::AuthenticationService>(ctx).unwrap();

        let maybe_account_info = authentication_service
            .find_account_info_by_name(&self.account_name)
            .await?;

        maybe_account_info.ok_or_else(|| {
            GqlError::Gql(
                Error::new("Account not resolved")
                    .extend_with(|_, eev| eev.set("name", self.account_name.to_string())),
            )
        })
    }

    #[graphql(skip)]
    #[inline]
    async fn get_full_account_info(
        &self,
        ctx: &Context<'_>,
    ) -> Result<&kamu_core::auth::AccountInfo> {
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
            kamu_core::auth::AccountType::User => AccountType::User,
            kamu_core::auth::AccountType::Organization => AccountType::Organization,
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
}

///////////////////////////////////////////////////////////////////////////////
