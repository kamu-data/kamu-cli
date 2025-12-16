// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{Account, AccountQuotaService, QuotaType, SetAccountQuotaError};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountQuotasMut<'a> {
    account: &'a Account,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> AccountQuotasMut<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    /// Setting quotas at the account level.
    #[tracing::instrument(level = "info", name = AccountQuotasMut_set_account_quotas, skip_all)]
    pub async fn set_account_quotas(
        &self,
        ctx: &Context<'_>,
        quotas: SetAccountQuotasInput,
    ) -> Result<SetAccountQuotasResult> {
        let quota_service = from_catalog_n!(ctx, dyn AccountQuotaService);
        let account_id = self.account.id.clone();

        let limit_total_bytes = quotas
            .storage
            .and_then(|s| s.limit_total_bytes)
            .ok_or_else(|| GqlError::gql("storage.limitTotalBytes is required"))?;

        quota_service
            .set_account_quota(&account_id, limit_total_bytes, QuotaType::storage_space())
            .await
            .map_err(map_set_quota_error)?;

        Ok(SetAccountQuotasResult { success: true })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct SetAccountQuotasInput {
    pub storage: Option<SetAccountQuotasStorageInput>,
}

#[derive(InputObject, Debug)]
pub struct SetAccountQuotasStorageInput {
    pub limit_total_bytes: Option<u64>,
}

#[derive(SimpleObject, Debug)]
pub struct SetAccountQuotasResult {
    success: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_set_quota_error(e: SetAccountQuotaError) -> GqlError {
    match e {
        SetAccountQuotaError::Save(inner) => GqlError::gql(format!("Failed to set quota: {inner}")),
        SetAccountQuotaError::Internal(inner) => inner.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
