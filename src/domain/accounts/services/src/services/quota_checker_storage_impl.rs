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
    QuotaExceededError,
    QuotaType,
    QuotaUnit,
};
use kamu_datasets::DatasetStatisticsService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn AccountQuotaStorageChecker)]
pub struct QuotaCheckerStorageImpl {
    quota_store: Arc<dyn AccountQuotaEventStore>,
    dataset_stats: Arc<dyn DatasetStatisticsService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl QuotaCheckerStorageImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccountQuotaStorageChecker for QuotaCheckerStorageImpl {
    async fn ensure_within_quota(
        &self,
        account_id: &odf::AccountID,
        incoming_bytes: u64,
    ) -> Result<(), QuotaExceededError> {
        let quota = match self
            .quota_store
            .get_quota_by_account_id(account_id, QuotaType::Space)
            .await
        {
            Ok(quota) => quota,
            // If quota is not configured - allow the operation
            Err(GetAccountQuotaError::NotFound(_)) => return Ok(()),
            Err(GetAccountQuotaError::Internal(e)) => return Err(QuotaExceededError::Internal(e)),
        };

        let used = match self
            .dataset_stats
            .get_total_statistic_by_account_id(account_id)
            .await
        {
            Ok(stat) => stat.get_size_summary(),
            Err(e) => return Err(QuotaExceededError::Internal(e)),
        };

        if quota.quota_payload.units != QuotaUnit::Bytes {
            return Err(QuotaExceededError::NotConfigured);
        }

        let limit = quota.quota_payload.value;

        if used + incoming_bytes > limit {
            Err(QuotaExceededError::Limit(kamu_accounts::LimitError {
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
