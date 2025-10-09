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

pub struct AccountFlowsMut<'a> {
    account: &'a Account,
}

#[Object]
impl<'a> AccountFlowsMut<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    pub async fn triggers(&self) -> AccountFlowTriggersMut {
        AccountFlowTriggersMut::new(self.account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
