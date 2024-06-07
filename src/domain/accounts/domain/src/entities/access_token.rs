// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

///////////////////////////////////////////////////////////////////////////////

use chrono::{DateTime, Utc};
use opendatafabric::AccountID;
use uuid::Uuid;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AccessToken {
    pub id: Uuid,
    pub token_name: String,
    pub token_hash: [u8; 16],
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub account_id: AccountID,
}
