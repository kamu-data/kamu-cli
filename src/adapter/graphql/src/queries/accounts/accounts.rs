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
    /// Returns account by its ID
    #[tracing::instrument(level = "info", name = Accounts_by_id, skip_all, fields(%account_id))]
    async fn by_id(&self, ctx: &Context<'_>, account_id: AccountID<'_>) -> Result<Option<Account>> {
        let account_service = from_catalog_n!(ctx, dyn kamu_accounts::AccountService);

        let account_id: odf::AccountID = account_id.into();
        let maybe_account_name = account_service.find_account_name_by_id(&account_id).await?;

        Ok(maybe_account_name
            .map(|account_name| Account::new(account_id.into(), account_name.into())))
    }

    /// Returns account by its name
    #[tracing::instrument(level = "info", name = Accounts_by_name, skip_all, fields(%name))]
    async fn by_name(&self, ctx: &Context<'_>, name: AccountName<'_>) -> Result<Option<Account>> {
        let account_service = from_catalog_n!(ctx, dyn kamu_accounts::AccountService);

        let account_name: odf::AccountName = name.into();

        let maybe_account = account_service.account_by_name(&account_name).await?;
        Ok(maybe_account.map(Account::from_account))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
