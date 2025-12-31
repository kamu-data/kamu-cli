// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{AccountQuotaQuery, QuotaType, QuotaUnit};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountQuotaPayload {
    pub units: QuotaUnit,
    pub value: u64,
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
            AccountQuotaEvent::AccountQuotaAdded(e) => e.quota_type.clone(),
            AccountQuotaEvent::AccountQuotaModified(e) => e.quota_type.clone(),
            AccountQuotaEvent::AccountQuotaRemoved(e) => e.quota_type.clone(),
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
