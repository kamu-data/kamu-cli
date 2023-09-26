// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::CurrentAccountSubject;
use url::Url;

use crate::services::{RemoteServerCredentialsService, RemoteServerLoginService};
use crate::{
    CLIError,
    Command,
    RemoteServerAccessToken,
    RemoteServerAccountCredentials,
    RemoteServerCredentialsScope,
    RemoteServerLoginError,
    DEFAULT_LOGIN_URL,
};

////////////////////////////////////////////////////////////////////////////////////////

pub struct LoginCommand {
    remote_server_login_service: Arc<RemoteServerLoginService>,
    remote_server_credentials_service: Arc<RemoteServerCredentialsService>,
    current_account_subject: Arc<CurrentAccountSubject>,
    scope: RemoteServerCredentialsScope,
    server: Option<Url>,
}

impl LoginCommand {
    pub fn new(
        remote_server_login_service: Arc<RemoteServerLoginService>,
        remote_server_credentials_service: Arc<RemoteServerCredentialsService>,
        current_account_subject: Arc<CurrentAccountSubject>,
        scope: RemoteServerCredentialsScope,
        server: Option<Url>,
    ) -> Self {
        Self {
            remote_server_login_service,
            remote_server_credentials_service,
            current_account_subject,
            scope,
            server,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for LoginCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let account_name = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(l) => l.account_name.clone(),
            CurrentAccountSubject::Anonymous(_) => panic!("Anonymous current account unexpected"),
        };

        let remote_server_frontend_url = self
            .server
            .as_ref()
            .map(|u| u.clone())
            .unwrap_or_else(|| Url::parse(DEFAULT_LOGIN_URL).unwrap());

        let login_callback_response = self
            .remote_server_login_service
            .login(&remote_server_frontend_url)
            .await
            .map_err(|e| match e {
                RemoteServerLoginError::CredentialsNotObtained => {
                    CLIError::usage_error(e.to_string())
                }
                RemoteServerLoginError::Internal(e) => CLIError::failure(e),
            })?;

        self.remote_server_credentials_service.save_credentials(
            self.scope,
            remote_server_frontend_url,
            login_callback_response.backend_url,
            account_name,
            RemoteServerAccountCredentials::AccessToken(RemoteServerAccessToken {
                access_token: login_callback_response.access_token,
            }),
        )?;

        eprintln!("{}", console::style("Login Succesful").green().bold());

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
