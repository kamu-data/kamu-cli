// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use email_utils::Email;
use merge::Merge;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::DUMMY_EMAIL_ADDRESS;
use crate::{
    AccountDisplayName,
    AccountType,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
    PROVIDER_PASSWORD,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DEFAULT_AVATAR_URL: &str = "https://avatars.githubusercontent.com/u/50896974?s=200&v=4";

#[skip_serializing_none]
#[derive(Default, Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PredefinedAccountsConfig {
    #[merge(strategy = merge::vec::append)]
    pub predefined: Vec<AccountConfig>,
}

impl PredefinedAccountsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sample() -> Self {
        Self::default()
    }

    pub fn single_tenant() -> Self {
        Self {
            predefined: vec![AccountConfig {
                id: Some(DEFAULT_ACCOUNT_ID.clone()),
                account_name: DEFAULT_ACCOUNT_NAME.clone(),
                password: None,
                account_type: AccountType::User,
                display_name: None,
                avatar_url: Some(String::from(DEFAULT_AVATAR_URL)),
                is_admin: true,
                registered_at: Utc::now(),
                provider: String::from(PROVIDER_PASSWORD),
                email: DUMMY_EMAIL_ADDRESS.clone(),
                treat_datasets_as_public: true,
            }],
        }
    }

    pub fn find_account_config_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Option<AccountConfig> {
        for account_config in &self.predefined {
            if account_config.account_name == *account_name {
                return Some(account_config.clone());
            }
        }

        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountConfig {
    // 'id' is auto-derived from `account_name` if omitted
    id: Option<odf::AccountID>,
    pub account_name: odf::AccountName,
    pub password: Option<String>,
    pub email: Email,
    // 'display_name' is auto-derived from `account_name` if omitted
    display_name: Option<AccountDisplayName>,
    #[serde(default = "AccountConfig::default_account_type")]
    pub account_type: AccountType,
    #[serde(default = "AccountConfig::default_provider")]
    pub provider: String,
    pub avatar_url: Option<String>,
    #[serde(default = "AccountConfig::default_registered_at")]
    pub registered_at: DateTime<Utc>,
    #[serde(default)]
    pub is_admin: bool,
    #[serde(default)]
    pub treat_datasets_as_public: bool,
}

impl AccountConfig {
    // TODO: Do not use the method outside of tests
    // #[cfg(any(feature = "testing", test))]
    pub fn test_config_from_name(account_name: odf::AccountName) -> Self {
        let email = Email::parse(&format!("{}@example.com", account_name.as_str())).unwrap();
        Self {
            id: None,
            account_name,
            password: None,
            email,
            display_name: None,
            account_type: Self::default_account_type(),
            provider: Self::default_provider(),
            avatar_url: None,
            registered_at: Self::default_registered_at(),
            is_admin: false,
            treat_datasets_as_public: false,
        }
    }

    pub fn set_password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }

    pub fn set_display_name(mut self, account_display_name: AccountDisplayName) -> Self {
        self.display_name = Some(account_display_name);
        self
    }

    pub fn get_id(&self) -> odf::AccountID {
        if let Some(id) = &self.id {
            id.clone()
        } else {
            odf::AccountID::new_seeded_ed25519(self.account_name.as_bytes())
        }
    }

    pub fn get_password(&self) -> String {
        if let Some(password) = &self.password {
            password.clone()
        } else {
            // Use same password as login name by default
            self.account_name.to_string()
        }
    }

    pub fn get_display_name(&self) -> AccountDisplayName {
        if let Some(display_name) = &self.display_name {
            display_name.clone()
        } else {
            self.account_name.to_string()
        }
    }

    pub fn default_account_type() -> AccountType {
        AccountType::User
    }

    pub fn default_provider() -> String {
        String::from(PROVIDER_PASSWORD)
    }

    pub fn default_registered_at() -> DateTime<Utc> {
        Utc::now()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
