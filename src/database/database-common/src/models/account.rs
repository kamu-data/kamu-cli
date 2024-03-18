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

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "account_origin", rename_all = "lowercase")]
pub enum AccountOrigin {
    Cli,
    Github,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
pub struct AccountModel {
    pub id: Uuid,
    pub email: String,
    pub account_name: String,
    pub display_name: String,
    pub origin: AccountOrigin,
    pub registered_at: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////
