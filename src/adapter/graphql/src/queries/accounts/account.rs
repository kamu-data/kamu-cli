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
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
};
use kamu_auth_rebac::{RebacService, RebacServiceExt};
use kamu_datasets::{DatasetAction, DatasetActionAuthorizer, DatasetActionAuthorizerExt};
use tokio::sync::OnceCell;

use super::AccountFlows;
use crate::prelude::*;
use crate::queries::{AccountAccessTokens, AccountUsage, Dataset, DatasetConnection};
use crate::utils;

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

impl Account {
    const DEFAULT_PER_PAGE: usize = 15;

    pub(crate) fn new(account_id: AccountID<'static>, account_name: AccountName<'static>) -> Self {
        Self {
            account_id,
            account_name,
            full_account_info: OnceCell::new(),
        }
    }

    pub(crate) fn from_account(account: kamu_accounts::Account) -> Self {
        Self {
            account_id: AccountID::from(account.id.clone()),
            account_name: account.account_name.clone().into(),
            full_account_info: OnceCell::new_with(Some(Box::new(account))),
        }
    }

    pub(crate) async fn from_account_id(
        ctx: &Context<'_>,
        account_id: odf::AccountID,
    ) -> Result<Self, InternalError> {
        let account_entity_data_loader = utils::get_account_entity_data_loader(ctx);

        let maybe_account = account_entity_data_loader
            .load_one(account_id.clone())
            .await
            .map_err(data_loader_error_mapper)?;

        if let Some(account) = maybe_account {
            Ok(Self::from_account(account))
        } else {
            Err(AccountNotFoundByIdError {
                account_id: account_id.clone(),
            }
            .int_err())
        }
    }

    pub(crate) async fn from_account_name(
        ctx: &Context<'_>,
        account_name: odf::AccountName,
    ) -> Result<Option<Self>, InternalError> {
        let account_entity_data_loader = utils::get_account_entity_data_loader(ctx);

        let maybe_account = account_entity_data_loader
            .load_one(account_name)
            .await
            .map_err(data_loader_error_mapper)?;

        Ok(maybe_account.map(Self::from_account))
    }

    // TODO: We should get rid of this method. Whenever resolving a dataset we
    // should get owner information that resides in the same table and avoid this
    // extra lookup by account name
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

    async fn resolve_full_account_info(&self, ctx: &Context<'_>) -> Result<kamu_accounts::Account> {
        let account_entity_data_loader = utils::get_account_entity_data_loader(ctx);

        let account_id: odf::AccountID = self.account_id.clone().into();
        let maybe_account = account_entity_data_loader
            .load_one(account_id)
            .await
            .map_err(data_loader_error_mapper)?;

        maybe_account.ok_or_else(|| {
            GqlError::Gql(
                Error::new("Account not resolved")
                    .extend_with(|_, eev| eev.set("id", self.account_id.to_string())),
            )
        })
    }

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

    pub(crate) fn account_name_internal(&self) -> &AccountName<'_> {
        &self.account_name
    }

    pub(crate) fn account_id_internal(&self) -> &odf::AccountID {
        &self.account_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Account {
    /// Unique and stable identifier of this account
    async fn id(&self) -> &AccountID<'_> {
        &self.account_id
    }

    /// Symbolic account name
    async fn account_name(&self) -> &AccountName<'_> {
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

    /// Account provider
    async fn account_provider(&self, ctx: &Context<'_>) -> Result<AccountProvider> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        let provider: kamu_accounts::AccountProvider =
            full_account_info.provider.parse().int_err()?;

        Ok(provider.into())
    }

    /// Email address
    async fn email(&self, ctx: &Context<'_>) -> Result<&str> {
        utils::check_logged_account_id_match(ctx, &self.account_id)?;

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
    async fn flows(&self, ctx: &Context<'_>) -> Result<AccountFlows<'_>> {
        let full_account_info = self.get_full_account_info(ctx).await?;

        Ok(AccountFlows::new(full_account_info))
    }

    /// Access to the access token management
    #[expect(clippy::unused_async)]
    async fn access_tokens(&self) -> Result<AccountAccessTokens<'_>> {
        Ok(AccountAccessTokens::new(&self.account_id))
    }

    /// Access to account usage statistic
    #[expect(clippy::unused_async)]
    async fn usage(&self) -> Result<AccountUsage<'_>> {
        Ok(AccountUsage::new(&self.account_id))
    }

    /// Returns datasets belonging to this account
    #[tracing::instrument(level = "info", name = Account_owned_datasets, skip_all)]
    async fn owned_datasets(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<DatasetConnection> {
        let (dataset_registry, dataset_action_authorizer) = from_catalog_n!(
            ctx,
            dyn kamu_datasets::DatasetRegistry,
            dyn DatasetActionAuthorizer
        );

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        use futures::TryStreamExt;

        let account_owned_datasets_stream =
            dataset_registry.all_dataset_handles_by_owner_id(&self.account_id);
        let readable_dataset_handles_stream = dataset_action_authorizer
            .filtered_datasets_stream(account_owned_datasets_stream, DatasetAction::Read);
        let mut accessible_datasets_handles = readable_dataset_handles_stream
            .try_collect::<Vec<_>>()
            .await?;

        let total_count = accessible_datasets_handles.len();

        accessible_datasets_handles.sort_by(|a, b| a.alias.cmp(&b.alias));

        let nodes = accessible_datasets_handles
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|handle| Dataset::new_access_checked(self.clone(), handle))
            .collect();

        Ok(DatasetConnection::new(nodes, page, per_page, total_count))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
