// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Accounts;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Accounts {
    /// Returns the account that is making the call (authorized subject)
    #[tracing::instrument(level = "info", name = Accounts_me, skip_all)]
    async fn me(&self, ctx: &Context<'_>) -> Result<Account> {
        use kamu_accounts::CurrentAccountSubject;

        let subject = from_catalog_n!(ctx, CurrentAccountSubject);

        match subject.as_ref() {
            CurrentAccountSubject::Logged(account) => Ok(Account::new(
                account.account_id.clone().into(),
                account.account_name.clone().into(),
            )),
            CurrentAccountSubject::Anonymous(_) => {
                Err(GqlError::gql(ANONYMOUS_ACCESS_FORBIDDEN_MESSAGE))
            }
        }
    }

    /// Returns an account by its ID, if found
    #[tracing::instrument(level = "info", name = Accounts_by_id, skip_all, fields(%account_id))]
    async fn by_id(&self, ctx: &Context<'_>, account_id: AccountID<'_>) -> Result<Option<Account>> {
        let account_service = from_catalog_n!(ctx, dyn kamu_accounts::AccountService);

        let account_id: odf::AccountID = account_id.into();
        let maybe_account_name = account_service.find_account_name_by_id(&account_id).await?;

        Ok(maybe_account_name
            .map(|account_name| Account::new(account_id.into(), account_name.into())))
    }

    /// Returns accounts by their IDs
    #[tracing::instrument(level = "info", name = Accounts_by_ids, skip_all, fields(?account_ids, ?skip_missing))]
    async fn by_ids(
        &self,
        ctx: &Context<'_>,
        account_ids: Vec<AccountID<'_>>,
        #[graphql(
            desc = "Whether to skip unresolved accounts or return an error if one or more are \
                    missing"
        )]
        skip_missing: bool,
    ) -> Result<Vec<Account>> {
        let account_service = from_catalog_n!(ctx, dyn kamu_accounts::AccountService);

        let domain_account_ids = account_ids.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let lookup = account_service
            .get_accounts_by_ids(&domain_account_ids)
            .await?;

        if !skip_missing && !lookup.not_found.is_empty() {
            return Err(GqlError::gql(format!(
                "Unresolved accounts: {}",
                itertools::join(lookup.not_found.iter().map(|(id, _)| id), ",")
            )));
        }

        Ok(lookup
            .found
            .into_iter()
            .map(Account::from_account)
            .collect())
    }

    /// Returns an account by its name, if found
    #[tracing::instrument(level = "info", name = Accounts_by_name, skip_all, fields(%name))]
    async fn by_name(&self, ctx: &Context<'_>, name: AccountName<'_>) -> Result<Option<Account>> {
        let account_service = from_catalog_n!(ctx, dyn kamu_accounts::AccountService);

        let account_name: odf::AccountName = name.into();

        let maybe_account = account_service.account_by_name(&account_name).await?;
        Ok(maybe_account.map(Account::from_account))
    }

    /// Returns accounts by their names
    #[tracing::instrument(level = "info", name = Accounts_by_names, skip_all, fields(?account_names, ?skip_missing))]
    async fn by_names(
        &self,
        ctx: &Context<'_>,
        account_names: Vec<AccountName<'_>>,
        #[graphql(
            desc = "Whether to skip unresolved accounts or return an error if one or more are \
                    missing"
        )]
        skip_missing: bool,
    ) -> Result<Vec<Account>> {
        let account_service = from_catalog_n!(ctx, dyn kamu_accounts::AccountService);

        let mut res = Vec::new();

        for account_name in account_names {
            let account_name = account_name.into();

            // TODO: PERF: Vectorize resolution
            match account_service.account_by_name(&account_name).await? {
                Some(account) => res.push(Account::from_account(account)),
                None if skip_missing => (),
                None => {
                    return Err(GqlError::gql(format!("Unresolved account: {account_name}")));
                }
            }
        }

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
