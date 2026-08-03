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
use url::Url;

use super::{DUMMY_EMAIL_ADDRESS, LoggedAccount};
use crate::{
    AccountDisplayName,
    AccountProvider,
    AccountType,
    DEFAULT_ACCOUNT_NAME,
    DEFAULT_ACCOUNT_PASSWORD,
    DEFAULT_PASSWORD_STR,
    Password,
    ProviderIdentityKey,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static DEFAULT_AVATAR_URL: std::sync::LazyLock<Url> = std::sync::LazyLock::new(|| {
    Url::parse("https://avatars.githubusercontent.com/u/50896974?s=200&v=4").unwrap()
});

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(setty::Config, setty::Default)]
pub struct PredefinedAccountsConfig {
    #[config(default, combine(merge))]
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
                id: None,
                private_key: None,
                account_name: DEFAULT_ACCOUNT_NAME.clone(),
                password: DEFAULT_ACCOUNT_PASSWORD.clone(),
                account_type: AccountType::User,
                display_name: None,
                avatar_url: Some(DEFAULT_AVATAR_URL.clone()),
                properties: vec![AccountPropertyName::IsAdmin],
                registered_at: None,
                provider: AccountProvider::Password.to_string(),
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

#[setty::derive(setty::Config, Clone, Copy, PartialEq, Eq)]
pub enum AccountPropertyName {
    CanProvisionAccounts,
    #[serde(rename = "Admin")]
    IsAdmin,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Embed the `AccountConfig::validate()` call
//       into setty validation mechanism (for whole struct).
/// The declarative account configuration used to register an account if one
/// does not already exist.
///
/// To update an existing account, either `id` or `private_key` must be
/// specified.
#[setty::derive(setty::Config, Clone)]
pub struct AccountConfig {
    /// May be omitted in favor of `private_key`.
    #[config(combine(replace))]
    pub id: Option<odf::AccountID>,

    /// Optional ed25519 private key. When set, `id` is derived from it
    /// (and must match `id` if both are present).
    #[config(combine(replace))]
    pub private_key: Option<odf::metadata::PrivateKey>,

    #[config(combine(replace))]
    pub account_name: odf::AccountName,

    // TODO: The password must be checked only for the "Password" provider, not for all of them.
    #[config(combine(replace))]
    pub password: Password,

    #[config(combine(replace))]
    pub email: Email,

    /// Auto-derived from `account_name` if omitted
    #[config(combine(replace))]
    pub display_name: Option<AccountDisplayName>,

    #[config(default = AccountType::User, combine(replace))]
    pub account_type: AccountType,

    #[config(default = AccountProvider::Password.to_string())]
    pub provider: String,

    #[config(combine(replace))]
    pub avatar_url: Option<Url>,

    // todo ref delete?
    // TODO: This should not be in config - we are mixing configuration and domain model here
    #[config(combine(replace))]
    pub registered_at: Option<DateTime<Utc>>,

    #[config(default)]
    pub properties: Vec<AccountPropertyName>,

    #[config(default = false)]
    pub treat_datasets_as_public: bool,
}

impl AccountConfig {
    //
    #[cfg(any(feature = "testing", test))]
    pub fn test_config_from_name(account_name: odf::AccountName) -> Self {
        let email = Email::parse(&format!("{account_name}@example.com")).unwrap();
        let password = Self::generate_password(&account_name);

        Self {
            id: None,
            private_key: None,
            account_name,
            password,
            email,
            display_name: None,
            account_type: Self::default_account_type(),
            provider: Self::default_provider(),
            avatar_url: None,
            registered_at: None,
            properties: Vec::new(),
            treat_datasets_as_public: false,
        }
    }

    // todo: action:
    // TODO: Do not use the method outside of tests
    // #[cfg(any(feature = "testing", test))]
    pub fn test_config_from_subject(subject: LoggedAccount) -> Self {
        let email = Email::parse(&format!("{}@example.com", subject.account_name)).unwrap();
        let password = Self::generate_password(&subject.account_name);

        Self {
            id: Some(subject.account_id),
            private_key: None,
            account_name: subject.account_name,
            password,
            email,
            display_name: None,
            account_type: Self::default_account_type(),
            provider: Self::default_provider(),
            avatar_url: None,
            registered_at: None,
            properties: Vec::new(),
            treat_datasets_as_public: false,
        }
    }

    pub fn set_password(mut self, password: Password) -> Self {
        self.password = password;
        self
    }

    pub fn set_display_name(mut self, account_display_name: AccountDisplayName) -> Self {
        self.display_name = Some(account_display_name);
        self
    }

    pub fn set_properties(mut self, properties: Vec<AccountPropertyName>) -> Self {
        self.properties = properties;
        self
    }

    pub fn set_registered_at(mut self, registered_at: DateTime<Utc>) -> Self {
        self.registered_at = Some(registered_at);
        self
    }

    /// Resolves account ID from `id` and/or `private_key`.
    /// Returns `None` when neither is set.
    ///
    /// NOTE: It is assumed that we call `validate()` first before using the
    ///       values.
    pub fn resolve_account_id(&self) -> Option<odf::AccountID> {
        match (&self.id, &self.private_key) {
            (Some(configured_id), Some(_private_key)) => {
                // NOTE: Important: In this method, we do not verify that the derived ID
                //       (from `private_key`) matches the configured ID.
                Some(configured_id.clone())
            }
            (Some(id), None) => Some(id.clone()),
            (None, Some(private_key)) => {
                let id = odf::AccountID::from_signing_key(private_key);
                Some(id)
            }
            (None, None) => None,
        }
    }

    pub fn provider_identity_key(&self) -> ProviderIdentityKey {
        self.account_name.to_string()
    }

    // TODO (refactoring): update?
    //     pub fn get_display_name(&self) -> Cow<&AccountDisplayName> {
    //     if let Some(display_name) = &self.display_name {
    //         Cow::Borrowed(display_name)
    //     } else {
    //         Cow::Owned(self.account_name.to_string())
    //     }
    // }
    pub fn get_display_name(&self) -> AccountDisplayName {
        if let Some(display_name) = &self.display_name {
            display_name.clone()
        } else {
            self.account_name.to_string()
        }
    }

    pub fn generate_password(account_name: &odf::AccountName) -> Password {
        Password::try_new(format!("{DEFAULT_PASSWORD_STR}:{account_name}")).unwrap()
    }

    pub fn get_id(&self) -> odf::AccountID {
        todo!("remove me")
    }

    pub fn validate(&self) -> Result<(), AccountConfigValidationError> {
        use std::str::FromStr;

        if let (Some(configured_id), Some(private_key)) = (&self.id, &self.private_key) {
            let derived_id = odf::AccountID::from_signing_key(private_key);

            if *configured_id != derived_id {
                return Err(AccountConfigValidationError::IdMismatch {
                    account_name: self.account_name.clone(),
                    configured_id: configured_id.clone(),
                    derived_id,
                });
            }
        }

        Email::parse(self.email.as_ref()).map_err(|_| {
            AccountConfigValidationError::InvalidEmail {
                account_name: self.account_name.clone(),
                email: self.email.to_string(),
            }
        })?;

        let provider = AccountProvider::from_str(&self.provider).map_err(|_| {
            AccountConfigValidationError::InvalidProvider {
                account_name: self.account_name.clone(),
                provider: self.provider.clone(),
            }
        })?;

        match provider {
            AccountProvider::OAuthGitHub | AccountProvider::Web3Wallet => {
                if self.private_key.is_some() {
                    return Err(AccountConfigValidationError::PrivateKeyNotAllowed {
                        account_name: self.account_name.clone(),
                        provider: self.provider.clone(),
                    });
                }
            }
            AccountProvider::Password => { /* nothing */ }
        }

        if let Some(id) = &self.id {
            match provider {
                AccountProvider::OAuthGitHub if id.as_did_odf().is_none() => {
                    return Err(AccountConfigValidationError::ExpectedDidOdf {
                        id: id.clone(),
                        account_name: self.account_name.clone(),
                    });
                }
                AccountProvider::Web3Wallet if id.as_did_pkh().is_none() => {
                    return Err(AccountConfigValidationError::ExpectedDidPkh {
                        id: id.clone(),
                        account_name: self.account_name.clone(),
                    });
                }
                _ => { /* nothing */ }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum AccountConfigValidationError {
    #[error(
        "Account '{account_name}': ID mismatch -- configured '{configured_id}', derived \
         '{derived_id}'"
    )]
    IdMismatch {
        account_name: odf::AccountName,
        configured_id: odf::AccountID,
        derived_id: odf::AccountID,
    },

    #[error("Account '{account_name}': invalid email '{email}'")]
    InvalidEmail {
        account_name: odf::AccountName,
        email: String,
    },

    #[error("Account '{account_name}': invalid provider '{provider}'")]
    InvalidProvider {
        account_name: odf::AccountName,
        provider: String,
    },

    #[error("Account '{account_name}': private key is not allowed for provider '{provider}'")]
    PrivateKeyNotAllowed {
        account_name: odf::AccountName,
        provider: String,
    },

    #[error(
        "Account '{account_name}': OAuthGitHub provider requires did:odf account id, got '{id}'"
    )]
    ExpectedDidOdf {
        account_name: odf::AccountName,
        id: odf::AccountID,
    },

    #[error(
        "Account '{account_name}': Web3Wallet provider requires did:pkh account id, got '{id}'"
    )]
    ExpectedDidPkh {
        account_name: odf::AccountName,
        id: odf::AccountID,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
