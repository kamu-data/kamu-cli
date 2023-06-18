// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::*;

///////////////////////////////////////////////////////////////////////////////

pub struct Accounts;

#[Object]
impl Accounts {
    /// Returns account by its ID
    #[allow(unused_variables)]
    async fn by_id(&self, _ctx: &Context<'_>, account_id: AccountID) -> Result<Option<Account>> {
        Ok(Some(Account::mock()))
    }

    /// Returns account by its name
    #[allow(unused_variables)]
    async fn by_name(&self, _ctx: &Context<'_>, name: String) -> Result<Option<Account>> {
        Ok(Some(Account::mock()))
    }
}

///////////////////////////////////////////////////////////////////////////////
