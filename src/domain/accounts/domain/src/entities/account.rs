// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

#[cfg(any(feature = "testing", test))]
use chrono::TimeZone;
use chrono::{DateTime, Utc};
use email_utils::Email;
use serde::{Deserialize, Serialize};

use crate::{AccountConfig, Password};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: have some length restrictions (0 < .. < limit)
pub type AccountDisplayName = String;

pub const DEFAULT_ACCOUNT_NAME_STR: &str = "kamu";
pub const DEFAULT_PASSWORD_STR: &str = "kamu.dev";

pub static DEFAULT_ACCOUNT_NAME: LazyLock<odf::AccountName> =
    LazyLock::new(|| odf::AccountName::new_unchecked(DEFAULT_ACCOUNT_NAME_STR));
pub static DEFAULT_ACCOUNT_ID: LazyLock<odf::AccountID> =
    LazyLock::new(|| odf::AccountID::new_seeded_ed25519(DEFAULT_ACCOUNT_NAME_STR.as_bytes()));
pub static DEFAULT_ACCOUNT_RESOURCE_ID: LazyLock<odf::ResourceID> =
    LazyLock::new(|| Account::seed_resource_id_from_name(DEFAULT_ACCOUNT_NAME_STR));
pub static DEFAULT_ACCOUNT_HANDLE: LazyLock<odf::AccountHandle> =
    LazyLock::new(|| odf::AccountHandle {
        id: *DEFAULT_ACCOUNT_RESOURCE_ID,
        did: DEFAULT_ACCOUNT_ID.clone(),
        name: DEFAULT_ACCOUNT_NAME.clone(),
    });
pub static DEFAULT_ACCOUNT_PASSWORD: LazyLock<Password> =
    LazyLock::new(|| Password::try_new(DEFAULT_PASSWORD_STR).unwrap());
pub static DUMMY_EMAIL_ADDRESS: LazyLock<Email> =
    LazyLock::new(|| Email::parse("kamu@example.com").unwrap());
#[cfg(any(feature = "testing", test))]
static DUMMY_REGISTRATION_TIME: LazyLock<DateTime<Utc>> =
    LazyLock::new(|| Utc.with_ymd_and_hms(2024, 4, 1, 12, 0, 0).unwrap());

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(strum::EnumString, strum::Display, strum::IntoStaticStr, Copy, Clone)]
pub enum AccountProvider {
    #[strum(serialize = "password")]
    Password,
    #[strum(serialize = "oauth_github")]
    OAuthGitHub,
    #[strum(serialize = "web3_wallet")]
    Web3Wallet,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Account {
    pub id: odf::AccountID,
    /// Artificial, stable id of the account *resource*. Persisted in the
    /// `accounts` table and threaded into `AccountHandle`. Long-term, `Account`
    /// will become a projection of the account resource identified by this id.
    pub resource_id: odf::ResourceID,
    pub account_name: odf::AccountName,
    pub email: Email,
    pub display_name: AccountDisplayName,
    pub account_type: AccountType,
    pub avatar_url: Option<String>,
    pub registered_at: DateTime<Utc>,
    pub provider: String,
    pub provider_identity_key: String,
}

impl Account {
    /// Mints a fresh random account-resource id. Use for genuinely new
    /// accounts created at runtime (e.g. via `CreateAccountUseCase`); updates
    /// must preserve the existing `resource_id`. Predefined/config-driven
    /// accounts use [`Account::seed_resource_id_from_name`] instead, since
    /// they must be stable across re-registration.
    ///
    /// TODO: interim measure. Once `Account` becomes a projection of an account
    /// resource, this id will be allocated by the resources framework as part
    /// of account-resource creation/reconciliation, not minted here.
    pub fn generate_resource_id() -> odf::ResourceID {
        odf::ResourceID::new(uuid::Uuid::new_v4())
    }

    /// Derives a deterministic account-resource id from an account name. Used
    /// for accounts whose identity is config-driven rather than minted at
    /// runtime — predefined accounts (`From<&AccountConfig>`), the default
    /// account, and dummy pre-workspace subjects — so the same name always
    /// resolves to the same `resource_id` across restarts/re-registration and
    /// test runs. Runtime-created accounts get a random id via
    /// [`Account::generate_resource_id`].
    ///
    /// TODO: interim measure, same as [`Account::generate_resource_id`] — goes
    /// away once account resources are reconciled for real.
    pub fn seed_resource_id_from_name(name: &str) -> odf::ResourceID {
        odf::ResourceID::new(uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            name.as_bytes(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<&Account> for odf::AccountHandle {
    fn from(account: &Account) -> Self {
        odf::AccountHandle {
            id: account.resource_id,
            did: account.id.clone(),
            name: account.account_name.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<&AccountConfig> for Account {
    fn from(account_config: &AccountConfig) -> Self {
        Account {
            id: account_config.get_id(),
            // Predefined accounts are config-driven, not "genuinely new" in the
            // sense `generate_resource_id` is meant for — seed deterministically
            // so re-registering the same config (e.g. across test runs, or a
            // server restart re-processing predefined accounts) yields the same
            // resource_id instead of a fresh random one each time.
            resource_id: Account::seed_resource_id_from_name(account_config.account_name.as_str()),
            account_name: account_config.account_name.clone(),
            email: account_config.email.clone(),
            display_name: account_config.get_display_name(),
            account_type: account_config.account_type,
            avatar_url: account_config.avatar_url.clone(),
            registered_at: account_config.registered_at.unwrap_or_else(Utc::now),
            provider: account_config.provider.clone(),
            provider_identity_key: account_config.account_name.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[cfg(any(feature = "testing", test))]
impl Account {
    pub fn dummy() -> Self {
        Self::test(DEFAULT_ACCOUNT_ID.clone(), DEFAULT_ACCOUNT_NAME_STR)
    }

    pub fn test(id: odf::AccountID, name: &str) -> Self {
        Self {
            id,
            resource_id: Account::seed_resource_id_from_name(name),
            account_name: odf::AccountName::new_unchecked(name),
            account_type: AccountType::User,
            display_name: name.to_string(),
            avatar_url: None,
            email: Email::parse(format!("{name}@example.com").as_str()).unwrap(),
            registered_at: DUMMY_REGISTRATION_TIME.to_utc(),
            provider: AccountProvider::Password.to_string(),
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
    pub resource_id: odf::ResourceID,
    pub account_name: String,
    pub email: String,
    pub display_name: String,
    pub account_type: AccountType,
    pub avatar_url: Option<String>,
    pub registered_at: DateTime<Utc>,
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
            resource_id: value.resource_id,
            account_name: odf::AccountName::new_unchecked(&value.account_name),
            email: Email::parse(&value.email).unwrap(),
            display_name: value.display_name,
            account_type: value.account_type,
            avatar_url: value.avatar_url,
            registered_at: value.registered_at,
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
            resource_id: value.resource_id,
            account_name: odf::AccountName::new_unchecked(&value.account_name),
            email: Email::parse(&value.email).unwrap(),
            display_name: value.display_name,
            account_type: value.account_type,
            avatar_url: value.avatar_url,
            registered_at: value.registered_at,
            provider: value.provider,
            provider_identity_key: value.provider_identity_key,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
