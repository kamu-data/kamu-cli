// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::Context;
use kamu_accounts::AccountService;

use crate::mutations::AccountMut;
use crate::prelude::*;
use crate::utils::{check_logged_account_id_match, check_logged_account_name_match};

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
        check_logged_account_id_match(ctx, &account_id)?;

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
        check_logged_account_name_match(ctx, &account_name)?;

        let account_service = from_catalog_n!(ctx, dyn AccountService);

        let account_maybe = account_service.account_by_name(&account_name).await?;
        Ok(account_maybe.map(AccountMut::new))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
