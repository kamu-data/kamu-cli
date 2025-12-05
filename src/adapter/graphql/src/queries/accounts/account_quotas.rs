// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{Account, AccountQuotaService, GetAccountQuotaError};

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

    /// Total bytes limit for this account (projected from quota events).
    pub async fn limit_total_bytes(&self, ctx: &Context<'_>) -> Result<u64> {
        let quota_service = from_catalog_n!(ctx, dyn AccountQuotaService);

        let quota = quota_service
            .get_account_storage_quota(&self.account.id)
            .await
            .map_err(map_get_quota_error)?;

        Ok(quota.quota_payload.value as u64)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_get_quota_error(e: GetAccountQuotaError) -> GqlError {
    match e {
        GetAccountQuotaError::NotFound(_) => GqlError::gql("Account quota not configured"),
        GetAccountQuotaError::Internal(inner) => inner.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
