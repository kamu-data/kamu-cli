// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{Account, AccountQuotaStorageChecker, GetStorageLimitError};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountQuotas<'a> {
    account: &'a Account,
}

#[Object]
impl<'a> AccountQuotas<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    /// User-level quotas
    pub async fn user(&self) -> AccountQuotasUsage<'_> {
        AccountQuotasUsage::new(self.account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountQuotasUsage<'a> {
    account: &'a Account,
}

#[Object]
impl<'a> AccountQuotasUsage<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    /// User-level quotas
    pub async fn storage(&self) -> AccountQuotasUsageStorage<'_> {
        AccountQuotasUsageStorage::new(self.account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountQuotasUsageStorage<'a> {
    account: &'a Account,
}

#[Object]
impl<'a> AccountQuotasUsageStorage<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    /// Total bytes limit for this account, or `null` when the storage of this
    /// account is unlimited.
    pub async fn limit_total_bytes(&self, ctx: &Context<'_>) -> Result<Option<u64>> {
        let quota_checker = from_catalog_n!(ctx, dyn AccountQuotaStorageChecker);

        quota_checker
            .get_storage_limit(&self.account.id)
            .await
            .map_err(map_get_storage_limit_error)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_get_storage_limit_error(e: GetStorageLimitError) -> GqlError {
    match e {
        GetStorageLimitError::NotConfigured => GqlError::gql("Account quota not configured"),
        GetStorageLimitError::Internal(inner) => inner.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
