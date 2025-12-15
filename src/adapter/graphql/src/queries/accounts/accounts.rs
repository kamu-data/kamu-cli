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
use crate::utils;

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
    pub async fn by_id(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID<'_>,
    ) -> Result<Option<Account>> {
        let account_entity_data_loader = utils::get_account_entity_data_loader(ctx);

        let account_id: odf::AccountID = account_id.into();
        let maybe_account = account_entity_data_loader
            .load_one(account_id)
            .await
            .map_err(data_loader_error_mapper)?;

        Ok(maybe_account.map(Account::from_account))
    }

    /// Returns accounts by their IDs.
    ///
    /// Order of results is guaranteed to match the inputs. Duplicate inputs
    /// will results in duplicate results.
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
        // TODO: PERF: GQL: DataLoader?

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

        // Reorder resolved accounts to match the order of inputs
        let found_accounts: Vec<kamu_accounts::Account> = {
            let found_accounts_map: std::collections::BTreeMap<
                odf::AccountID,
                kamu_accounts::Account,
            > = lookup
                .found
                .into_iter()
                .map(|a| (a.id.clone(), a))
                .collect();

            domain_account_ids
                .iter()
                .filter_map(|id| found_accounts_map.get(*id).cloned())
                .collect()
        };

        let accounts = found_accounts
            .into_iter()
            .map(Account::from_account)
            .collect();

        Ok(accounts)
    }

    /// Returns an account by its name, if found
    #[tracing::instrument(level = "info", name = Accounts_by_name, skip_all, fields(%name))]
    async fn by_name(&self, ctx: &Context<'_>, name: AccountName<'_>) -> Result<Option<Account>> {
        let account_entity_data_loader = utils::get_account_entity_data_loader(ctx);

        let account_name: odf::AccountName = name.into();
        let maybe_account = account_entity_data_loader
            .load_one(account_name)
            .await
            .map_err(data_loader_error_mapper)?;

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
        // TODO: PERF: GQL: DataLoader?

        let account_service = from_catalog_n!(ctx, dyn kamu_accounts::AccountService);

        let domain_account_names = account_names.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let lookup = account_service
            .get_accounts_by_names(&domain_account_names)
            .await?;

        if !skip_missing && !lookup.not_found.is_empty() {
            return Err(GqlError::gql(format!(
                "Unresolved accounts: {}",
                itertools::join(lookup.not_found.iter().map(|(name, _)| name), ",")
            )));
        }

        // Reorder resolved accounts to match the order of inputs
        let found_accounts: Vec<kamu_accounts::Account> = {
            let found_accounts_map: std::collections::BTreeMap<
                odf::AccountName,
                kamu_accounts::Account,
            > = lookup
                .found
                .into_iter()
                .map(|a| (a.account_name.clone(), a))
                .collect();

            domain_account_names
                .iter()
                .filter_map(|name| found_accounts_map.get(*name).cloned())
                .collect()
        };

        let accounts = found_accounts
            .into_iter()
            .map(Account::from_account)
            .collect();

        Ok(accounts)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
