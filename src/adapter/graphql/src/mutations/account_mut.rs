// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric as odf;

use super::AccountFlowsMut;
use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct AccountMut {
    account_id: odf::AccountName,
}

#[Object]
impl AccountMut {
    #[graphql(skip)]
    pub fn new(account_id: odf::AccountName) -> Self {
        Self { account_id }
    }

    /// Access to the mutable flow configurations of this account
    async fn flows(&self) -> AccountFlowsMut {
        AccountFlowsMut::new(self.account_id.clone())
    }
}
