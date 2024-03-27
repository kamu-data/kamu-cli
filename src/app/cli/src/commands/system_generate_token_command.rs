// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::*;
use kamu::AuthenticationServiceImpl;
use opendatafabric::AccountName;

use crate::accounts::{PasswordProviderCredentials, LOGIN_METHOD_PASSWORD};
use crate::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////

pub struct GenerateTokenCommand {
    auth_service: Arc<AuthenticationServiceImpl>,
    login: String,
    gh_access_token: Option<String>,
    expiration_time_sec: usize,
}

impl GenerateTokenCommand {
    pub fn new(
        auth_service: Arc<AuthenticationServiceImpl>,
        login: String,
        gh_access_token: Option<String>,
        expiration_time_sec: usize,
    ) -> Self {
        Self {
            auth_service,
            login,
            gh_access_token,
            expiration_time_sec,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for GenerateTokenCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        let (login_method, provider_credentials_json) =
            if let Some(gh_access_token) = &self.gh_access_token {
                let creds = serde_json::to_string(&kamu_adapter_oauth::GithubProviderCredentials {
                    access_token: gh_access_token.clone(),
                })
                .int_err()?;
                (kamu_adapter_oauth::LOGIN_METHOD_GITHUB, creds)
            } else {
                let creds = serde_json::to_string(&PasswordProviderCredentials {
                    account_name: AccountName::try_from(&self.login).unwrap(),
                })
                .int_err()?;
                (LOGIN_METHOD_PASSWORD, creds)
            };

        let token = self.auth_service.make_access_token(
            self.login.clone(),
            login_method,
            provider_credentials_json,
            self.expiration_time_sec,
        )?;

        println!("{token}");
        Ok(())
    }
}
