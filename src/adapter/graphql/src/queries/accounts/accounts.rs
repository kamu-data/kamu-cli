// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric as odf;

use crate::prelude::*;
use crate::queries::Account;

///////////////////////////////////////////////////////////////////////////////

pub struct Accounts;

#[Object]
impl Accounts {
    /// Returns account by its ID
    #[allow(unused_variables)]
    #[allow(clippy::unused_async)]
    async fn by_id(&self, ctx: &Context<'_>, account_id: AccountID) -> Result<Option<Account>> {
        let authentication_service =
            from_catalog::<dyn kamu_accounts::AuthenticationService>(ctx).unwrap();

        let account_id: odf::AccountID = account_id.into();
        let maybe_account_name = authentication_service
            .find_account_name_by_id(&account_id)
            .await?;

        Ok(maybe_account_name
            .map(|account_name| Account::new(account_id.into(), account_name.into())))
    }

    /// Returns account by its name
    async fn by_name(&self, ctx: &Context<'_>, name: AccountName) -> Result<Option<Account>> {
        let authentication_service =
            from_catalog::<dyn kamu_accounts::AuthenticationService>(ctx).unwrap();

        let account_name: odf::AccountName = name.into();

        let maybe_account = authentication_service
            .account_by_name(&account_name)
            .await?;
        Ok(maybe_account.map(Account::from_account))
    }
}

///////////////////////////////////////////////////////////////////////////////
