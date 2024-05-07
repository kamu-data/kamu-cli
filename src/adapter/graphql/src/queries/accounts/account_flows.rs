// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric as odf;

use super::AccountFlowRuns;
use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

pub struct AccountFlows {
    account_name: odf::AccountName,
}

#[Object]
impl AccountFlows {
    #[graphql(skip)]
    pub fn new(account_name: odf::AccountName) -> Self {
        Self { account_name }
    }

    /// Returns interface for flow runs queries
    async fn runs(&self) -> AccountFlowRuns {
        AccountFlowRuns::new(self.account_name.clone())
    }
}

///////////////////////////////////////////////////////////////////////////////
