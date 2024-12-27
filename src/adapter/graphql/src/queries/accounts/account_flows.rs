// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::Account;

use super::{AccountFlowRuns, AccountFlowTriggers};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlows {
    account: Account,
}

#[Object]
impl AccountFlows {
    #[graphql(skip)]
    pub fn new(account: Account) -> Self {
        Self { account }
    }

    /// Returns interface for flow runs queries
    async fn runs(&self) -> AccountFlowRuns {
        AccountFlowRuns::new(self.account.clone())
    }

    /// Returns interface for flow triggers queries
    async fn triggers(&self) -> AccountFlowTriggers {
        AccountFlowTriggers::new(self.account.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
