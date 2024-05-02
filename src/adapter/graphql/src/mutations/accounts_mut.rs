// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth::AuthenticationService;

use crate::mutations::AccountMut;
use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

pub struct AccountsMut;

///////////////////////////////////////////////////////////////////////////////

#[Object]
impl AccountsMut {
    /// Returns a mutable account by its name
    async fn by_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName,
    ) -> Result<Option<AccountMut>> {
        let authentication_service = from_catalog::<dyn AuthenticationService>(ctx).unwrap();

        let account_maybe = authentication_service
            .find_account_info_by_name(&account_name)
            .await?;

        Ok(account_maybe.map(|account| AccountMut::new(account.account_name)))
    }
}
