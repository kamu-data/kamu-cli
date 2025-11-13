// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::Account;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountQuotas<'a> {
    account: &'a Account,
}

#[Object]
impl<'a> AccountQuotas<'a> {
    #[graphql(skip)]
    #[expect(unused)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    /// User-level quotas
    pub async fn user(&self) -> AccountQuotasUser<'_> {
        AccountQuotasUser::new(self.account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountQuotasUser<'a> {
    account: &'a Account,
}

#[Object]
impl<'a> AccountQuotasUser<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    /// User-level quotas
    pub async fn storage(&self) -> AccountQuotasUserStorage<'_> {
        AccountQuotasUserStorage::new(self.account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountQuotasUserStorage<'a> {
    #[expect(unused)]
    account: &'a Account,
}

#[Object]
impl<'a> AccountQuotasUserStorage<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    /// Total bytes used by this account.
    pub async fn utilized_total_bytes(&self) -> Result<u64> {
        todo!()
    }

    /// Total bytes limit for this account.
    pub async fn limit_total_bytes(&self) -> Result<u64> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
