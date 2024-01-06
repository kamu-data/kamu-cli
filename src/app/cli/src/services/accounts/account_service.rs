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
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::auth::{self, AccountInfo, AccountType};
use opendatafabric::{AccountName, FAKE_ACCOUNT_ID};
use serde::{Deserialize, Serialize};

use crate::accounts::models::*;
use crate::config::UsersConfig;

/////////////////////////////////////////////////////////////////////////////////////////

pub const LOGIN_METHOD_PASSWORD: &str = "password";

/////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountService {
    pub predefined_accounts: HashMap<String, AccountInfo>,
    pub allow_login_unknown: bool,
}

#[component(pub)]
#[interface(dyn auth::AuthenticationProvider)]
impl AccountService {
    pub fn new(users_config: Arc<UsersConfig>) -> Self {
        let mut predefined_accounts: HashMap<String, AccountInfo> = HashMap::new();

        for predefined_account in &users_config.predefined {
            predefined_accounts.insert(
                predefined_account.account_name.to_string(),
                predefined_account.clone(),
            );
        }

        Self {
            predefined_accounts,
            allow_login_unknown: users_config.allow_login_unknown.unwrap_or(true),
        }
    }

    fn default_account_name(multitenant_workspace: bool) -> String {
        if multitenant_workspace {
            whoami::username()
        } else {
            String::from(auth::DEFAULT_ACCOUNT_NAME)
        }
    }

    fn default_user_name(multitenant_workspace: bool) -> String {
        if multitenant_workspace {
            whoami::realname()
        } else {
            String::from(auth::DEFAULT_ACCOUNT_NAME)
        }
    }

    pub fn current_account_indication(
        arg_matches: &ArgMatches,
        multitenant_workspace: bool,
    ) -> CurrentAccountIndication {
        let default_account_name: String =
            AccountService::default_account_name(multitenant_workspace);
        let default_user_name: String = AccountService::default_user_name(multitenant_workspace);

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

    fn find_account_info_impl(&self, account_name: &String) -> Option<AccountInfo> {
        // The account might be predefined in the configuration
        self.predefined_accounts
            .get(account_name)
            .map(|an| an.clone())
    }

    fn get_account_info_impl(
        &self,
        account_name: &String,
    ) -> Result<AccountInfo, auth::RejectedCredentialsError> {
        // The account might be predefined in the configuration
        match self.predefined_accounts.get(account_name) {
            // Use the predefined record
            Some(account_info) => Ok(account_info.clone()),

            None => {
                // If configuration allows login unknown users, pretend this is an unknown user
                // without avatar and with the name identical to login
                if self.allow_login_unknown {
                    Ok(AccountInfo {
                        account_id: FAKE_ACCOUNT_ID.to_string(),
                        account_name: AccountName::new_unchecked(account_name),
                        account_type: AccountType::User,
                        display_name: account_name.clone(),
                        avatar_url: None,
                        is_admin: false,
                    })
                } else {
                    // Otherwise we don't recognized this user between predefined
                    Err(auth::RejectedCredentialsError::new(
                        "Login of unknown accounts is disabled".to_string(),
                    ))
                }
            }
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
        let account_info = self
            .get_account_info_impl(&password_login_credentials.login)
            .map_err(|e| auth::ProviderLoginError::RejectedCredentials(e))?;

        // Store login as provider credentials
        let provider_credentials = PasswordProviderCredentials {
            account_name: account_info.account_name.clone(),
        };

        Ok(auth::ProviderLoginResponse {
            provider_credentials_json: serde_json::to_string::<PasswordProviderCredentials>(
                &provider_credentials,
            )
            .int_err()?,
            account_info: account_info.into(),
        })
    }

    async fn account_info_by_token(
        &self,
        provider_credentials_json: String,
    ) -> Result<AccountInfo, InternalError> {
        let provider_credentials = serde_json::from_str::<PasswordProviderCredentials>(
            &provider_credentials_json.as_str(),
        )
        .int_err()?;

        let account_info = self
            .get_account_info_impl(&provider_credentials.account_name.to_string())
            .int_err()?;

        Ok(account_info)
    }

    async fn find_account_info_by_name<'a>(
        &'a self,
        account_name: &'a AccountName,
    ) -> Result<Option<AccountInfo>, InternalError> {
        Ok(self.find_account_info_impl(&account_name.into()))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PasswordProviderCredentials {
    pub account_name: AccountName,
}

///////////////////////////////////////////////////////////////////////////////
