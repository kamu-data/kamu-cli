// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::Context;
use kamu_accounts::AuthenticationService;

use crate::mutations::AccountMut;
use crate::prelude::*;
use crate::utils::{check_logged_account_id_match, check_logged_account_name_match};

///////////////////////////////////////////////////////////////////////////////

pub struct AccountsMut;

///////////////////////////////////////////////////////////////////////////////

#[Object]
impl AccountsMut {
    /// Returns a mutable account by its id
    async fn by_id(&self, ctx: &Context<'_>, account_id: AccountID) -> Result<Option<AccountMut>> {
        let authentication_service = from_catalog::<dyn AuthenticationService>(ctx).unwrap();
        check_logged_account_id_match(ctx, &account_id)?;

        let account_maybe = authentication_service.account_by_id(&account_id).await?;
        Ok(account_maybe.map(AccountMut::new))
    }

    /// Returns a mutable account by its name
    async fn by_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName,
    ) -> Result<Option<AccountMut>> {
        let authentication_service = from_catalog::<dyn AuthenticationService>(ctx).unwrap();
        check_logged_account_name_match(ctx, &account_name)?;

        let account_maybe = authentication_service
            .account_by_name(&account_name)
            .await?;
        Ok(account_maybe.map(AccountMut::new))
    }
}
