// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use super::{AccountQuota, AccountQuotaEvent, AccountQuotaQuery};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountQuotaState {
    pub quota: AccountQuota,
}

#[async_trait::async_trait]
impl event_sourcing::Projection for AccountQuotaState {
    type Query = AccountQuotaQuery;
    type Event = AccountQuotaEvent;

    fn apply(
        state: Option<Self>,
        event: Self::Event,
    ) -> Result<Self, event_sourcing::ProjectionError<Self>> {
        match event {
            AccountQuotaEvent::AccountQuotaAdded(e) => {
                if state.is_some() {
                    return Err(event_sourcing::ProjectionError::new(
                        state,
                        AccountQuotaEvent::AccountQuotaAdded(e),
                    ));
                }

                Ok(AccountQuotaState {
                    quota: AccountQuota {
                        id: e.quota_id,
                        account_id: e.account_id,
                        quota_type: e.quota_type,
                        quota_payload: e.quota_payload,
                        active: true,
                        created_at: e.event_time,
                        updated_at: e.event_time,
                    },
                })
            }
            AccountQuotaEvent::AccountQuotaModified(e) => {
                if let Some(mut state) = state {
                    if state.quota.id != e.quota_id {
                        return Err(event_sourcing::ProjectionError::new(
                            Some(state),
                            AccountQuotaEvent::AccountQuotaModified(e),
                        ));
                    }
                    state.quota.quota_payload = e.quota_payload;
                    state.quota.updated_at = e.event_time;
                    Ok(state)
                } else {
                    Err(event_sourcing::ProjectionError::new(
                        None,
                        AccountQuotaEvent::AccountQuotaModified(e),
                    ))
                }
            }
            AccountQuotaEvent::AccountQuotaRemoved(e) => {
                if let Some(mut state) = state {
                    if state.quota.id != e.quota_id {
                        return Err(event_sourcing::ProjectionError::new(
                            Some(state),
                            AccountQuotaEvent::AccountQuotaRemoved(e),
                        ));
                    }
                    state.quota.active = false;
                    state.quota.updated_at = e.event_time;
                    Ok(state)
                } else {
                    Err(event_sourcing::ProjectionError::new(
                        None,
                        AccountQuotaEvent::AccountQuotaRemoved(e),
                    ))
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
