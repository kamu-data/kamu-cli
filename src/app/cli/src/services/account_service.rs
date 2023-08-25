// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use clap::ArgMatches;
use dill::component;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::{auth, CurrentAccountSubject};
use opendatafabric::AccountName;
use serde::{Deserialize, Serialize};

use crate::UsersConfig;

/////////////////////////////////////////////////////////////////////////////////////////

pub const LOGIN_METHOD_PASSWORD: &str = "password";

/////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountService {
    pub predefined_accounts: HashMap<String, auth::AccountInfo>,
}

#[component(pub)]
impl AccountService {
    pub fn new(users_config: Arc<UsersConfig>) -> Self {
        let mut predefined_accounts: HashMap<String, auth::AccountInfo> = HashMap::new();
        for predefined_account in &users_config.predefined {
            predefined_accounts.insert(
                predefined_account.login.to_string(),
                predefined_account.clone(),
            );
        }

        Self {
            predefined_accounts,
        }
    }

    fn default_account_name() -> String {
        whoami::username()
    }

    fn default_user_name() -> String {
        whoami::realname()
    }

    pub fn current_account_indication(arg_matches: &ArgMatches) -> CurrentAccountIndication {
        let default_account_name: String = AccountService::default_account_name();
        let default_user_name: String = AccountService::default_user_name();

        let (current_account, user_name, specified_explicitly) =
            if let Some(account) = arg_matches.get_one::<String>("account") {
                (
                    account.clone(),
                    if account.eq(&default_account_name) {
                        default_account_name
                    } else {
                        account.clone() // Use account as user name, when there
                                        // is no data
                    },
                    true,
                )
            } else {
                (default_account_name, default_user_name, false)
            };

        CurrentAccountIndication::new(current_account, user_name, specified_explicitly)
    }

    pub fn related_account_indication(sub_matches: &ArgMatches) -> RelatedAccountIndication {
        let target_account =
            if let Some(target_account) = sub_matches.get_one::<String>("target-account") {
                TargetAccountSelection::Specific {
                    account_name: target_account.clone(),
                }
            } else if sub_matches.get_flag("all-accounts") {
                TargetAccountSelection::AllUsers
            } else {
                TargetAccountSelection::Current
            };

        RelatedAccountIndication::new(target_account)
    }

    fn get_account_info_impl(&self, login: &String) -> auth::AccountInfo {
        // The account might be predefined in the configuration
        match self.predefined_accounts.get(login) {
            // Use the predefined record
            Some(account_info) => account_info.clone(),

            // Pretend this is an unknown user without avatar and with the name identical to login
            None => auth::AccountInfo {
                login: AccountName::new_unchecked(login),
                name: login.clone(),
                avatar_url: None,
            },
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl auth::AuthenticationProvider for AccountService {
    fn login_method(&self) -> &'static str {
        LOGIN_METHOD_PASSWORD
    }

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<auth::ProviderLoginResponse, auth::ProviderLoginError> {
        // Decode credentials
        let password_login_credentials =
            serde_json::from_str::<PasswordLoginCredentials>(login_credentials_json.as_str())
                .map_err(|e| {
                    auth::ProviderLoginError::InvalidCredentials(
                        auth::InvalidCredentialsError::new(Box::new(e)),
                    )
                })?;

        // For now password should match the login, this is enough for CLI demo needs
        if password_login_credentials
            .password
            .ne(&password_login_credentials.login)
        {
            return Err(auth::ProviderLoginError::RejectedCredentials(
                auth::RejectedCredentialsError::new("Invalid login or password".into()),
            ));
        }

        // The account might be predefined in the configuration
        let account_info = self.get_account_info_impl(&password_login_credentials.login);

        // Store login as provider credentials
        let provider_credentials = PasswordProviderCredentials {
            account_name: account_info.login.clone(),
        };

        Ok(auth::ProviderLoginResponse {
            provider_credentials_json: serde_json::to_string::<PasswordProviderCredentials>(
                &provider_credentials,
            )
            .int_err()?,
            account_info: account_info.into(),
        })
    }

    async fn get_account_info(
        &self,
        provider_credentials_json: String,
    ) -> Result<auth::AccountInfo, InternalError> {
        let provider_credentials = serde_json::from_str::<PasswordProviderCredentials>(
            &provider_credentials_json.as_str(),
        )
        .int_err()?;

        let account_info =
            self.get_account_info_impl(&provider_credentials.account_name.to_string());

        Ok(account_info)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct RelatedAccountIndication {
    pub target_account: TargetAccountSelection,
}

impl RelatedAccountIndication {
    pub fn new(target_account: TargetAccountSelection) -> Self {
        Self { target_account }
    }

    pub fn is_explicit(&self) -> bool {
        self.target_account != TargetAccountSelection::Current
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum TargetAccountSelection {
    Current,
    Specific { account_name: String },
    AllUsers,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CurrentAccountIndication {
    pub account_name: AccountName,
    pub user_name: String,
    pub specified_explicitly: bool,
}

impl CurrentAccountIndication {
    pub fn new<A, U>(account_name: A, user_name: U, specified_explicitly: bool) -> Self
    where
        A: Into<String>,
        U: Into<String>,
    {
        Self {
            account_name: AccountName::try_from(account_name.into()).unwrap(),
            user_name: user_name.into(),
            specified_explicitly,
        }
    }

    pub fn is_explicit(&self) -> bool {
        self.specified_explicitly
    }

    pub fn as_current_account_subject(&self) -> CurrentAccountSubject {
        CurrentAccountSubject::new(auth::AccountInfo {
            login: AccountName::from(self.account_name.clone()),
            name: self.user_name.clone(),
            avatar_url: None,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordLoginCredentials {
    pub login: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PasswordProviderCredentials {
    pub account_name: AccountName,
}

///////////////////////////////////////////////////////////////////////////////
