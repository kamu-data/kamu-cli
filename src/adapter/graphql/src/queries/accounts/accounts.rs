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
    async fn by_id(&self, _ctx: &Context<'_>, account_id: AccountID) -> Result<Option<Account>> {
        panic!("Resolving accounts by ID is not supported yet");
    }

    /// Returns account by its name
    #[allow(unused_variables)]
    async fn by_name(&self, ctx: &Context<'_>, name: AccountName) -> Result<Option<Account>> {
        let authentication_service =
            from_catalog::<dyn kamu_core::auth::AuthenticationService>(ctx).unwrap();

        let account_name: odf::AccountName = name.into();

        let maybe_account_info = authentication_service
            .find_account_info_by_name(&account_name)
            .await?;
        Ok(maybe_account_info.map(|ai| Account::from_account_info(ai)))
    }
}

///////////////////////////////////////////////////////////////////////////////
