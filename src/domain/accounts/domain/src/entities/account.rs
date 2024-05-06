// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, TimeZone, Utc};
use lazy_static::lazy_static;
use opendatafabric::{AccountID, AccountName};
use serde::{Deserialize, Serialize};

///////////////////////////////////////////////////////////////////////////////

// TODO: have some length restrictions (0 < .. < limit)
pub type AccountDisplayName = String;

pub const DEFAULT_ACCOUNT_NAME_STR: &str = "kamu";

lazy_static! {
    pub static ref DEFAULT_ACCOUNT_NAME: AccountName =
        AccountName::new_unchecked(DEFAULT_ACCOUNT_NAME_STR);
    pub static ref DEFAULT_ACCOUNT_ID: AccountID =
        AccountID::new_seeded_ed25519(DEFAULT_ACCOUNT_NAME_STR.as_bytes());
    static ref DUMMY_REGISTRATION_TIME: DateTime<Utc> =
        Utc.with_ymd_and_hms(2024, 4, 1, 12, 0, 0).unwrap();
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Account {
    pub id: AccountID,
    pub account_name: AccountName,
    pub email: Option<String>,
    pub display_name: AccountDisplayName,
    pub account_type: AccountType,
    pub avatar_url: Option<String>,
    pub registered_at: DateTime<Utc>,
    pub is_admin: bool,
    pub provider: String,
    pub provider_identity_key: String,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, sqlx::Type, PartialEq, Eq, Serialize, Deserialize)]
#[sqlx(type_name = "account_type", rename_all = "lowercase")]
pub enum AccountType {
    User,
    Organization,
}

///////////////////////////////////////////////////////////////////////////////

pub const PROVIDER_PASSWORD: &str = "password";

///////////////////////////////////////////////////////////////////////////////

impl Account {
    pub fn dummy() -> Self {
        Self {
            id: DEFAULT_ACCOUNT_ID.clone(),
            account_name: DEFAULT_ACCOUNT_NAME.clone(),
            account_type: AccountType::User,
            display_name: DEFAULT_ACCOUNT_NAME_STR.to_string(),
            avatar_url: None,
            email: None,
            registered_at: DUMMY_REGISTRATION_TIME.to_utc(),
            is_admin: false,
            provider: String::from(PROVIDER_PASSWORD),
            provider_identity_key: String::from(DEFAULT_ACCOUNT_NAME_STR),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
