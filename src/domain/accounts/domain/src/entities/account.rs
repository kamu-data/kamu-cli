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
use serde::{Deserialize, Serialize};

use crate::AccountConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: have some length restrictions (0 < .. < limit)
pub type AccountDisplayName = String;

pub const DEFAULT_ACCOUNT_NAME_STR: &str = "kamu";

lazy_static! {
    pub static ref DEFAULT_ACCOUNT_NAME: odf::AccountName =
        odf::AccountName::new_unchecked(DEFAULT_ACCOUNT_NAME_STR);
    pub static ref DEFAULT_ACCOUNT_ID: odf::AccountID =
        odf::AccountID::new_seeded_ed25519(DEFAULT_ACCOUNT_NAME_STR.as_bytes());
    static ref DUMMY_REGISTRATION_TIME: DateTime<Utc> =
        Utc.with_ymd_and_hms(2024, 4, 1, 12, 0, 0).unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Account {
    pub id: odf::AccountID,
    pub account_name: odf::AccountName,
    pub email: Option<String>,
    pub display_name: AccountDisplayName,
    pub account_type: AccountType,
    pub avatar_url: Option<String>,
    pub registered_at: DateTime<Utc>,
    // TODO: Private Datasets: absorb the `is_admin` attribute from the Accounts domain
    //       https://github.com/kamu-data/kamu-cli/issues/766
    pub is_admin: bool,
    pub provider: String,
    pub provider_identity_key: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<&AccountConfig> for Account {
    fn from(account_config: &AccountConfig) -> Self {
        Account {
            id: account_config.get_id(),
            account_name: account_config.account_name.clone(),
            email: account_config.email.clone(),
            display_name: account_config.get_display_name(),
            account_type: account_config.account_type,
            avatar_url: account_config.avatar_url.clone(),
            registered_at: account_config.registered_at,
            is_admin: account_config.is_admin,
            provider: account_config.provider.clone(),
            provider_identity_key: account_config.account_name.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    feature = "sqlx",
    derive(sqlx::Type),
    sqlx(type_name = "account_type", rename_all = "lowercase")
)]
pub enum AccountType {
    User,
    Organization,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const PROVIDER_PASSWORD: &str = "password";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(any(feature = "testing", test))]
impl Account {
    pub fn dummy() -> Self {
        Self::test(DEFAULT_ACCOUNT_ID.clone(), DEFAULT_ACCOUNT_NAME_STR)
    }

    pub fn test(id: odf::AccountID, name: &str) -> Self {
        Self {
            id,
            account_name: odf::AccountName::new_unchecked(name),
            account_type: AccountType::User,
            display_name: name.to_string(),
            avatar_url: None,
            email: None,
            registered_at: DUMMY_REGISTRATION_TIME.to_utc(),
            is_admin: false,
            provider: String::from(PROVIDER_PASSWORD),
            provider_identity_key: String::from(name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[reusable::reusable(account_row_model)]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct AccountRowModel {
    pub id: odf::AccountID,
    pub account_name: String,
    pub email: Option<String>,
    pub display_name: String,
    pub account_type: AccountType,
    pub avatar_url: Option<String>,
    pub registered_at: DateTime<Utc>,
    pub is_admin: bool,
    pub provider: String,
    pub provider_identity_key: String,
}

#[cfg(feature = "sqlx")]
#[reusable::reuse(account_row_model)]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct AccountWithTokenRowModel {
    pub token_hash: Vec<u8>,
}

#[cfg(feature = "sqlx")]
impl From<AccountRowModel> for Account {
    fn from(value: AccountRowModel) -> Self {
        Account {
            id: value.id,
            account_name: odf::AccountName::new_unchecked(&value.account_name),
            email: value.email,
            display_name: value.display_name,
            account_type: value.account_type,
            avatar_url: value.avatar_url,
            registered_at: value.registered_at,
            is_admin: value.is_admin,
            provider: value.provider,
            provider_identity_key: value.provider_identity_key,
        }
    }
}

#[cfg(feature = "sqlx")]
impl From<AccountWithTokenRowModel> for Account {
    fn from(value: AccountWithTokenRowModel) -> Self {
        Account {
            id: value.id,
            account_name: odf::AccountName::new_unchecked(&value.account_name),
            email: value.email,
            display_name: value.display_name,
            account_type: value.account_type,
            avatar_url: value.avatar_url,
            registered_at: value.registered_at,
            is_admin: value.is_admin,
            provider: value.provider,
            provider_identity_key: value.provider_identity_key,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
