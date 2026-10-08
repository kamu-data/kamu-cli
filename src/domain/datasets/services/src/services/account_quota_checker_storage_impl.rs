// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_accounts::{
    AccountQuotaEventStore,
    AccountQuotaStorageChecker,
    GetAccountQuotaError,
    GetStorageLimitError,
    QuotaError,
    QuotaType,
    QuotaUnit,
};
use kamu_auth_rebac::{RebacService, RebacServiceExt};
use kamu_datasets::DatasetStatisticsService;

use crate::QuotaDefaultsConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn AccountQuotaStorageChecker)]
pub struct AccountQuotaCheckerStorageImpl {
    quota_store: Arc<dyn AccountQuotaEventStore>,
    dataset_stats: Arc<dyn DatasetStatisticsService>,
    rebac_service: Arc<dyn RebacService>,
    quota_defaults_config: QuotaDefaultsConfig,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl AccountQuotaStorageChecker for AccountQuotaCheckerStorageImpl {
    #[tracing::instrument(
        name = AccountQuotaCheckerStorageImpl_get_storage_limit,
        level = "debug",
        skip_all,
    )]
    async fn get_storage_limit(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Option<u64>, GetStorageLimitError> {
        // Admin accounts are never limited, whatever quota is stored for them
        if self
            .rebac_service
            .is_account_admin(account_id)
            .await
            .int_err()?
        {
            return Ok(None);
        }

        match self
            .quota_store
            .get_quota_by_account_id(account_id, &QuotaType::storage_space())
            .await
        {
            Ok(quota) => {
                if quota.quota_payload.units != QuotaUnit::Bytes {
                    return Err(GetStorageLimitError::NotConfigured);
                }
                Ok(Some(quota.quota_payload.value))
            }
            Err(GetAccountQuotaError::NotFound(_)) => Ok(Some(self.quota_defaults_config.storage)),
            Err(GetAccountQuotaError::Internal(e)) => Err(GetStorageLimitError::Internal(e)),
        }
    }

    #[tracing::instrument(
        name = AccountQuotaCheckerStorageImpl_ensure_within_quota,
        level = "debug",
        skip_all,
    )]
    async fn ensure_within_quota(
        &self,
        account_id: &odf::AccountID,
        incoming_bytes: u64,
    ) -> Result<(), QuotaError> {
        let Some(limit) = self.get_storage_limit(account_id).await? else {
            return Ok(());
        };

        let used = match self
            .dataset_stats
            .get_total_statistic_by_account_id(account_id)
            .await
        {
            Ok(stat) => stat.get_size_summary(),
            Err(e) => return Err(QuotaError::Internal(e)),
        };

        if used + incoming_bytes > limit {
            Err(QuotaError::Exceeded(kamu_accounts::QuotaExceededError {
                used,
                incoming: incoming_bytes,
                limit,
            }))
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
