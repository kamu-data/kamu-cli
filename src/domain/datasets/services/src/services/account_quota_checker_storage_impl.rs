// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_accounts::{
    AccountQuotaEventStore,
    AccountQuotaStorageChecker,
    GetAccountQuotaError,
    QuotaError,
    QuotaType,
    QuotaUnit,
};
use kamu_datasets::DatasetStatisticsService;

use crate::QuotaDefaultsConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn AccountQuotaStorageChecker)]
pub struct AccountQuotaCheckerStorageImpl {
    quota_store: Arc<dyn AccountQuotaEventStore>,
    dataset_stats: Arc<dyn DatasetStatisticsService>,
    quota_defaults_config: QuotaDefaultsConfig,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AccountQuotaCheckerStorageImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccountQuotaStorageChecker for AccountQuotaCheckerStorageImpl {
    async fn ensure_within_quota(
        &self,
        account_id: &odf::AccountID,
        incoming_bytes: u64,
    ) -> Result<(), QuotaError> {
        let quota_type = QuotaType::storage_space();

        let quota_payload_value = match self
            .quota_store
            .get_quota_by_account_id(account_id, &quota_type)
            .await
        {
            Ok(quota) => {
                if quota.quota_payload.units != QuotaUnit::Bytes {
                    return Err(QuotaError::NotConfigured);
                }
                quota.quota_payload.value
            }
            Err(GetAccountQuotaError::NotFound(_)) => self.quota_defaults_config.storage,
            Err(GetAccountQuotaError::Internal(e)) => return Err(QuotaError::Internal(e)),
        };

        let used = match self
            .dataset_stats
            .get_total_statistic_by_account_id(account_id)
            .await
        {
            Ok(stat) => stat.get_size_summary(),
            Err(e) => return Err(QuotaError::Internal(e)),
        };

        if used + incoming_bytes > quota_payload_value {
            Err(QuotaError::Exceeded(kamu_accounts::QuotaExceededError {
                used,
                incoming: incoming_bytes,
                limit: quota_payload_value,
            }))
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
