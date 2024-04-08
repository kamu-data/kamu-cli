// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use opendatafabric::AccountID;
use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, Serialize, Deserialize, sqlx::Type, PartialEq, Eq)]
#[sqlx(type_name = "account_origin", rename_all = "lowercase")]
pub enum AccountOrigin {
    Cli,
    Github,
}

#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
#[allow(dead_code)]
pub struct AccountModel {
    pub id: AccountID,
    pub email: String,
    pub account_name: String,
    pub display_name: String,
    pub origin: AccountOrigin,
    pub registered_at: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////
