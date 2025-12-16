// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ErrorIntoInternal;
use kamu_accounts::{
    AccountQuotaAdded,
    AccountQuotaEvent,
    AccountQuotaEventStore,
    AccountQuotaModified,
    AccountQuotaPayload,
    AccountQuotaQuery,
    AccountQuotaService,
    GetAccountQuotaError,
    QuotaType,
    QuotaUnit,
    SetAccountQuotaError,
};
use time_source::SystemTimeSource;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn AccountQuotaService)]
pub struct AccountQuotaServiceImpl {
    account_quota_store: Arc<dyn AccountQuotaEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccountQuotaService for AccountQuotaServiceImpl {
    async fn set_account_storage_quota(
        &self,
        account_id: &odf::AccountID,
        limit_bytes: u64,
    ) -> Result<(), SetAccountQuotaError> {
        let query = AccountQuotaQuery {
            account_id: account_id.clone(),
            quota_type: QuotaType::storage_space(),
        };

        let now = self.time_source.now();

        let maybe_current = self
            .account_quota_store
            .get_quota_by_account_id(account_id, QuotaType::storage_space())
            .await;

        let prev_event_id = self
            .account_quota_store
            .last_event_id(&query)
            .await
            .map_err(|e| SetAccountQuotaError::Internal(e.int_err()))?;

        let payload = AccountQuotaPayload {
            units: QuotaUnit::Bytes,
            value: limit_bytes,
        };

        let event = match maybe_current {
            Ok(quota) => AccountQuotaEvent::AccountQuotaModified(AccountQuotaModified {
                event_time: now,
                quota_id: quota.id,
                account_id: account_id.clone(),
                quota_type: QuotaType::storage_space(),
                quota_payload: payload,
            }),
            Err(GetAccountQuotaError::NotFound(_)) => {
                AccountQuotaEvent::AccountQuotaAdded(AccountQuotaAdded {
                    event_time: now,
                    quota_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    quota_type: QuotaType::storage_space(),
                    quota_payload: payload,
                })
            }
            Err(e) => return Err(SetAccountQuotaError::Internal(e.int_err())),
        };

        self.account_quota_store
            .save_quota_events(&query, prev_event_id, vec![event])
            .await?;

        Ok(())
    }

    async fn get_account_storage_quota(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<kamu_accounts::AccountQuota, GetAccountQuotaError> {
        self.account_quota_store
            .get_quota_by_account_id(account_id, QuotaType::storage_space())
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
