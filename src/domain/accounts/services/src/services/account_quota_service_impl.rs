// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use event_sourcing::{Aggregate, TryLoadError};
use internal_error::ErrorIntoInternal;
use kamu_accounts::{
    AccountQuotaAdded,
    AccountQuotaEvent,
    AccountQuotaEventStore,
    AccountQuotaModified,
    AccountQuotaPayload,
    AccountQuotaQuery,
    AccountQuotaService,
    AccountQuotaState,
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
    async fn set_account_quota(
        &self,
        account_id: &odf::AccountID,
        limit: u64,
        quota_type: QuotaType,
    ) -> Result<(), SetAccountQuotaError> {
        let query = AccountQuotaQuery {
            account_id: account_id.clone(),
            quota_type: quota_type.clone(),
        };

        let now = self.time_source.now();

        let (maybe_current, prev_event_id) =
            match Aggregate::<AccountQuotaState, dyn AccountQuotaEventStore>::try_load(
                &query,
                self.account_quota_store.as_ref(),
            )
            .await
            {
                Ok(Some(agg)) => {
                    let prev_event_id = agg.last_stored_event_id();
                    let state = agg.into_state();
                    let maybe_current = if state.quota.active {
                        Some(state.quota)
                    } else {
                        None
                    };
                    (maybe_current, prev_event_id)
                }
                Ok(None) => (None, None),
                Err(TryLoadError::ProjectionError(err)) => {
                    return Err(SetAccountQuotaError::Internal(err.int_err()));
                }
                Err(TryLoadError::Internal(err)) => {
                    return Err(SetAccountQuotaError::Internal(err));
                }
            };

        let payload = AccountQuotaPayload {
            units: QuotaUnit::Bytes,
            value: limit,
        };

        let event = match maybe_current {
            Some(quota) => AccountQuotaEvent::AccountQuotaModified(AccountQuotaModified {
                event_time: now,
                quota_id: quota.id,
                account_id: account_id.clone(),
                quota_type: quota_type.clone(),
                quota_payload: payload,
            }),
            None => AccountQuotaEvent::AccountQuotaAdded(AccountQuotaAdded {
                event_time: now,
                quota_id: Uuid::new_v4(),
                account_id: account_id.clone(),
                quota_type: quota_type.clone(),
                quota_payload: payload,
            }),
        };

        self.account_quota_store
            .save_quota_events(&query, prev_event_id, vec![event])
            .await?;

        Ok(())
    }

    async fn get_account_quota(
        &self,
        account_id: &odf::AccountID,
        quota_type: QuotaType,
    ) -> Result<kamu_accounts::AccountQuota, kamu_accounts::GetAccountQuotaError> {
        self.account_quota_store
            .get_quota_by_account_id(account_id, quota_type)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
