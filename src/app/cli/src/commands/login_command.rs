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
use opendatafabric::AccountName;
use url::Url;

use crate::services::{RemoteServerCredentialsService, RemoteServerLoginService};
use crate::{
    CLIError,
    Command,
    RemoteServerAccessToken,
    RemoteServerAccountCredentials,
    RemoteServerCredentialsFindDetails,
    RemoteServerCredentialsScope,
    RemoteServerLoginError,
    RemoteServerValidateLoginError,
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

    async fn new_login(
        &self,
        remote_server_frontend_url: Url,
        account_name: AccountName,
    ) -> Result<(), CLIError> {
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

        Ok(())
    }

    async fn validate_login(
        &self,
        details: RemoteServerCredentialsFindDetails,
    ) -> Result<(), RemoteServerValidateLoginError> {
        self.remote_server_login_service
            .validate_login_credentials(&details.server_backend_url, &details.account_credentials)
            .await
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

        if let Some(details) = self.remote_server_credentials_service.find_by_frontend_url(
            self.scope,
            &remote_server_frontend_url,
            &account_name,
        ) {
            match self.validate_login(details).await {
                Ok(_) => {
                    eprintln!("{}", console::style("Credentials valid").green().bold());
                    return Ok(());
                }
                Err(RemoteServerValidateLoginError::ExpiredCredentials(_)) => {
                    eprintln!("{}", console::style("Credentials expired").yellow().bold());
                    Ok(())
                }
                Err(RemoteServerValidateLoginError::InvalidCredentials(e)) => {
                    Err(CLIError::failure(e))
                }
                Err(RemoteServerValidateLoginError::Internal(e)) => Err(CLIError::critical(e)),
            }?;
        }

        self.new_login(remote_server_frontend_url, account_name)
            .await?;
        eprintln!("{}", console::style("Login successful").green().bold());
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
