// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::Account;

use super::AccountFlowsMut;
use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct AccountMut {
    account: Account,
}

#[Object]
impl AccountMut {
    #[graphql(skip)]
    pub fn new(account: Account) -> Self {
        Self { account }
    }

    /// Access to the mutable flow configurations of this account
    async fn flows(&self) -> AccountFlowsMut {
        AccountFlowsMut::new(self.account.clone())
    }
}
