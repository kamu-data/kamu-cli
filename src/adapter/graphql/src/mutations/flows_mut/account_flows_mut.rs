// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::Account;

use super::AccountFlowTriggersMut;
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowsMut {
    account: Account,
}

#[Object]
impl AccountFlowsMut {
    #[graphql(skip)]
    pub fn new(account: Account) -> Self {
        Self { account }
    }

    async fn triggers(&self) -> AccountFlowTriggersMut {
        AccountFlowTriggersMut::new(self.account.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
