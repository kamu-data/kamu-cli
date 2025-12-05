// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Display, Formatter};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QuotaType {
    Space,
}

impl Display for QuotaType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaType::Space => write!(f, "SPACE"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum QuotaUnit {
    Bytes,
}

impl Display for QuotaUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QuotaUnit::Bytes => write!(f, "Bytes"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountQuota {
    pub id: Uuid,
    pub account_id: odf::AccountID,
    pub quota_type: QuotaType,
    pub quota_payload: AccountQuotaPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountQuotaPayload {
    pub units: QuotaUnit,
    pub value: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountQuotaQuery {
    pub account_id: odf::AccountID,
    pub quota_type: QuotaType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountQuotaEvent {
    AccountQuotaAdded(AccountQuotaAdded),
    AccountQuotaModified(AccountQuotaModified),
    AccountQuotaRemoved(AccountQuotaRemoved),
}

impl event_sourcing::ProjectionEvent<AccountQuotaQuery> for AccountQuotaEvent {
    fn matches_query(&self, query: &AccountQuotaQuery) -> bool {
        self.account_id() == query.account_id && self.quota_type() == query.quota_type
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AccountQuotaState {
    pub quota: Option<AccountQuota>,
}

#[async_trait::async_trait]
impl event_sourcing::Projection for AccountQuotaState {
    type Query = AccountQuotaQuery;
    type Event = AccountQuotaEvent;

    fn apply(
        state: Option<Self>,
        event: Self::Event,
    ) -> Result<Self, event_sourcing::ProjectionError<Self>> {
        let mut state = state.unwrap_or_default();

        match event {
            AccountQuotaEvent::AccountQuotaAdded(e) => {
                if state.quota.is_some() {
                    return Err(event_sourcing::ProjectionError::new(
                        Some(state.clone()),
                        AccountQuotaEvent::AccountQuotaAdded(e),
                    ));
                }

                state.quota = Some(AccountQuota {
                    id: e.quota_id,
                    account_id: e.account_id,
                    quota_type: e.quota_type,
                    quota_payload: e.quota_payload,
                    created_at: e.event_time,
                    updated_at: e.event_time,
                });
            }
            AccountQuotaEvent::AccountQuotaModified(e) => {
                if let Some(quota) = &mut state.quota {
                    if quota.id != e.quota_id {
                        return Err(event_sourcing::ProjectionError::new(
                            Some(state.clone()),
                            AccountQuotaEvent::AccountQuotaModified(e),
                        ));
                    }
                    quota.quota_payload = e.quota_payload;
                    quota.updated_at = e.event_time;
                } else {
                    return Err(event_sourcing::ProjectionError::new(
                        None,
                        AccountQuotaEvent::AccountQuotaModified(e),
                    ));
                }
            }
            AccountQuotaEvent::AccountQuotaRemoved(_) => {
                state.quota = None;
            }
        }

        Ok(state)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountQuotaAdded {
    pub event_time: DateTime<Utc>,
    pub quota_id: Uuid,
    pub account_id: odf::AccountID,
    pub quota_type: QuotaType,
    pub quota_payload: AccountQuotaPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountQuotaModified {
    pub event_time: DateTime<Utc>,
    pub quota_id: Uuid,
    pub account_id: odf::AccountID,
    pub quota_type: QuotaType,
    pub quota_payload: AccountQuotaPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountQuotaRemoved {
    pub event_time: DateTime<Utc>,
    pub quota_id: Uuid,
    pub account_id: odf::AccountID,
    pub quota_type: QuotaType,
}

impl AccountQuotaEvent {
    pub fn typename(&self) -> &'static str {
        match self {
            AccountQuotaEvent::AccountQuotaAdded(_) => "AccountQuotaAdded",
            AccountQuotaEvent::AccountQuotaModified(_) => "AccountQuotaModified",
            AccountQuotaEvent::AccountQuotaRemoved(_) => "AccountQuotaRemoved",
        }
    }

    pub fn account_id(&self) -> odf::AccountID {
        match self {
            AccountQuotaEvent::AccountQuotaAdded(e) => e.account_id.clone(),
            AccountQuotaEvent::AccountQuotaModified(e) => e.account_id.clone(),
            AccountQuotaEvent::AccountQuotaRemoved(e) => e.account_id.clone(),
        }
    }

    pub fn quota_type(&self) -> QuotaType {
        match self {
            AccountQuotaEvent::AccountQuotaAdded(e) => e.quota_type,
            AccountQuotaEvent::AccountQuotaModified(e) => e.quota_type,
            AccountQuotaEvent::AccountQuotaRemoved(e) => e.quota_type,
        }
    }

    pub fn quota_id(&self) -> Uuid {
        match self {
            AccountQuotaEvent::AccountQuotaAdded(e) => e.quota_id,
            AccountQuotaEvent::AccountQuotaModified(e) => e.quota_id,
            AccountQuotaEvent::AccountQuotaRemoved(e) => e.quota_id,
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            AccountQuotaEvent::AccountQuotaAdded(e) => e.event_time,
            AccountQuotaEvent::AccountQuotaModified(e) => e.event_time,
            AccountQuotaEvent::AccountQuotaRemoved(e) => e.event_time,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
