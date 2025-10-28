// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::Account;

use super::{AccountFlowProcesses, AccountFlowRuns, AccountFlowTriggers};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlows<'a> {
    account: &'a Account,
}

#[Object]
impl<'a> AccountFlows<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    /// Returns interface for flow processes summary queries
    pub async fn processes(&self) -> AccountFlowProcesses<'_> {
        AccountFlowProcesses::new(self.account)
    }

    /// Returns interface for flow runs queries
    pub async fn runs(&self) -> AccountFlowRuns<'_> {
        AccountFlowRuns::new(self.account)
    }

    /// Returns interface for flow triggers queries
    pub async fn triggers(&self) -> AccountFlowTriggers<'_> {
        AccountFlowTriggers::new(self.account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
