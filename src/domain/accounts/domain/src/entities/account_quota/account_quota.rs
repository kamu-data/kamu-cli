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

use super::{AccountQuotaPayload, QuotaType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountQuota {
    pub id: Uuid,
    pub account_id: odf::AccountID,
    pub quota_type: QuotaType,
    pub quota_payload: AccountQuotaPayload,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct AccountQuotaQuery {
    pub account_id: odf::AccountID,
    pub quota_type: QuotaType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
